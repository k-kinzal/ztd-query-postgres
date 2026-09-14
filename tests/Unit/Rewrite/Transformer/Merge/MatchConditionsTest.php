<?php

declare(strict_types=1);

namespace Tests\Unit\Rewrite\Transformer\Merge;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Rewrite\Transformer\Merge\MatchConditions::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Cte\HeaderParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Cte\IdentifierReferences::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Cte\PrefixMerge::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Cte\ShadowDependencies::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Merge\ActionClause::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Merge\BranchTokens::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Merge\RelationTarget::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Merge\StatementParts::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Relation\FromClause::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Relation\RelationReference::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Cte\PgSqlCteShadowComposer::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Merge\PgSqlMergeActionKind::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Merge\PgSqlMergeClause::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Merge\PgSqlMergeMatchKind::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Merge\PgSqlMergeParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Merge\PgSqlMergeStatement::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Relation\PgSqlSelectRelationParser::class)]
final class MatchConditionsTest extends TestCase
{
    public function testEffectiveConditions(): void
    {
        $sql = 'MERGE INTO users u USING incoming s ON u.id = s.id WHEN MATCHED AND s.active THEN UPDATE SET name = s.name WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)';
        $statement = (new \ZtdQuery\Platform\Postgres\Sql\Merge\PgSqlMergeParser())->parse($sql);

        self::assertSame(['(s.active)', '(TRUE)'], (new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\Merge\MatchConditions())->effectiveConditions($statement));
    }
}
