<?php

declare(strict_types=1);

namespace Tests\Unit\Rewrite\Transformer\Insert;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Rewrite\Transformer\Insert\ValueProjection::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Conflict\ColumnSet::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Relation\FromClause::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Relation\RelationReference::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Statement\Classification::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Statement\ConflictClause::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Statement\Identifiers::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Statement\InsertSource::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Statement\TableDefinitionClauses::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Value\PgSqlCastRenderer::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Conflict\PgSqlConflictTarget::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Relation\PgSqlSelectRelationParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PostgreSqlLexicalMasker::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Value\CastTypeMapper::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Lexing\CommentSpan::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Lexing\QuotedSpan::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Value\NativeCastTarget::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Transformer\InsertRowRenderer::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Transformer\Insert\ExpressionCast::class)]
final class ValueProjectionTest extends TestCase
{
    public function testRenderPreservesSequenceProgressAcrossCommittedProjections(): void
    {
        $allocator = new \ZtdQuery\Rewrite\ShadowIdentityAllocator();
        $allocator->beginProjection();
        $projection = new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\Insert\ValueProjection(new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser(), $allocator, new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\InsertRowRenderer(), new \ZtdQuery\Platform\Postgres\Sql\Value\PgSqlCastRenderer());
        $table = ['rows' => [['id' => 7]], 'identityStrategies' => ['id' => \ZtdQuery\Schema\Key\IdentityGenerationStrategy::Sequence], 'columnTypes' => ['id' => new \ZtdQuery\Schema\ColumnDeclaration(\ZtdQuery\Schema\ColumnTypeFamily::INTEGER, 'INTEGER')]];
        $sql = $projection->render("INSERT INTO users (name) VALUES ('Ada'), ('Bob')", 'users', ['id', 'name'], ['name'], $table);
        self::assertSame("SELECT CAST(1 AS INTEGER) AS \"id\", 'Ada' AS \"name\" UNION ALL SELECT CAST(8 AS INTEGER) AS \"id\", 'Bob' AS \"name\"", $sql);
        $allocator->commitProjection();
        $allocator->beginProjection();
        $next = $projection->render("INSERT INTO users (name) VALUES ('Cy')", 'users', ['id', 'name'], ['name'], $table);
        self::assertStringContainsString('CAST(9 AS INTEGER)', $next);
    }
}
