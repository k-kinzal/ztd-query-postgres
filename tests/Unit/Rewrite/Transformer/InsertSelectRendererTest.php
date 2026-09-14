<?php

declare(strict_types=1);

namespace Tests\Unit\Rewrite\Transformer;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Platform\Postgres\Rewrite\Transformer\InsertSelectRenderer;
use ZtdQuery\Platform\Postgres\Sql\PgSqlIdentifierQuoter;

#[CoversClass(InsertSelectRenderer::class)]
#[UsesClass(PgSqlIdentifierQuoter::class)]
#[CoversClass(\ZtdQuery\Platform\Postgres\Rewrite\Transformer\Insert\SelectProjection::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Conflict\ColumnSet::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Cte\HeaderParser::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Cte\IdentifierReferences::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Cte\PrefixMerge::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Cte\ShadowDependencies::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Relation\FromClause::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Relation\RelationReference::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Statement\Classification::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Statement\ConflictClause::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Statement\Identifiers::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Statement\InsertSource::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Statement\TableDefinitionClauses::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Conflict\PgSqlConflictTarget::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Cte\PgSqlCteShadowComposer::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlParser::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Relation\PgSqlSelectRelationParser::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\PostgreSqlLexicalMasker::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Lexing\CommentSpan::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Lexing\QuotedSpan::class)]
final class InsertSelectRendererTest extends TestCase
{
    public function testRendersPostgreSqlProjectionFromDialectNeutralPlan(): void
    {
        $sql = (new InsertSelectRenderer())->render(
            'SELECT DISTINCT name, COUNT(*) FROM users GROUP BY name',
            ['id', 'name', 'count', 'status'],
            ['name', 'count'],
            ['status' => "'active'"],
            ['id' => 8],
        );

        self::assertSame(
            'WITH "__ztd_insert_source" ("__ztd_insert_0", "__ztd_insert_1") AS ('
            . 'SELECT DISTINCT name, COUNT(*) FROM users GROUP BY name) '
            . 'SELECT 8 + ROW_NUMBER() OVER () - 1 AS "id", "__ztd_insert_0" AS "name", '
            . '"__ztd_insert_1" AS "count", \'active\' AS "status" FROM "__ztd_insert_source"',
            $sql,
        );
    }

    public function testRenderGeneratedIdentityRendersGeneratedIdentityExpression(): void
    {
        $renderer = new InsertSelectRenderer();

        self::assertSame('1 + ROW_NUMBER() OVER () - 1', $renderer->renderGeneratedIdentity(1));
    }


}
