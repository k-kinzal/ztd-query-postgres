<?php

declare(strict_types=1);

namespace Tests\Unit\Sql\Merge;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Sql\Merge\ActionClause::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Merge\BranchTokens::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Merge\RelationTarget::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Merge\PgSqlMergeActionKind::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Merge\PgSqlMergeClause::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Merge\PgSqlMergeMatchKind::class)]
final class ActionClauseTest extends TestCase
{
    public function testParseClause(): void
    {
        self::assertEquals(new \ZtdQuery\Platform\Postgres\Sql\Merge\PgSqlMergeClause(matchKind: \ZtdQuery\Platform\Postgres\Sql\Merge\PgSqlMergeMatchKind::Matched, conditionSql: null, actionKind: \ZtdQuery\Platform\Postgres\Sql\Merge\PgSqlMergeActionKind::Delete, assignments: [], insertColumns: [], insertValues: []), (new \ZtdQuery\Platform\Postgres\Sql\Merge\ActionClause())->parseClause('MERGE INTO users u USING incoming s ON u.id = s.id WHEN MATCHED AND s.active THEN UPDATE SET name = s.name WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)', 'WHEN MATCHED THEN DELETE'));

        self::assertEquals(new \ZtdQuery\Platform\Postgres\Sql\Merge\PgSqlMergeClause(matchKind: \ZtdQuery\Platform\Postgres\Sql\Merge\PgSqlMergeMatchKind::NotMatched, conditionSql: null, actionKind: \ZtdQuery\Platform\Postgres\Sql\Merge\PgSqlMergeActionKind::DoNothing, assignments: [], insertColumns: [], insertValues: []), (new \ZtdQuery\Platform\Postgres\Sql\Merge\ActionClause())->parseClause('MERGE INTO users u USING incoming s ON u.id = s.id WHEN MATCHED AND s.active THEN UPDATE SET name = s.name WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)', 'WHEN NOT MATCHED THEN DO NOTHING'));

        self::assertEquals(new \ZtdQuery\Platform\Postgres\Sql\Merge\PgSqlMergeClause(matchKind: \ZtdQuery\Platform\Postgres\Sql\Merge\PgSqlMergeMatchKind::Matched, conditionSql: 's.active', actionKind: \ZtdQuery\Platform\Postgres\Sql\Merge\PgSqlMergeActionKind::Update, assignments: ['name' => 's.name'], insertColumns: [], insertValues: []), (new \ZtdQuery\Platform\Postgres\Sql\Merge\ActionClause())->parseClause('MERGE INTO users u USING incoming s ON u.id = s.id WHEN MATCHED AND s.active THEN UPDATE SET name = s.name WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)', 'WHEN MATCHED AND s.active THEN UPDATE SET name = s.name'));

        self::assertEquals(new \ZtdQuery\Platform\Postgres\Sql\Merge\PgSqlMergeClause(matchKind: \ZtdQuery\Platform\Postgres\Sql\Merge\PgSqlMergeMatchKind::NotMatched, conditionSql: null, actionKind: \ZtdQuery\Platform\Postgres\Sql\Merge\PgSqlMergeActionKind::Insert, assignments: [], insertColumns: ['id', 'name'], insertValues: ['s.id', 's.name']), (new \ZtdQuery\Platform\Postgres\Sql\Merge\ActionClause())->parseClause('MERGE INTO users u USING incoming s ON u.id = s.id WHEN MATCHED AND s.active THEN UPDATE SET name = s.name WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)', 'WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)'));
    }

    public function testParseAssignments(): void
    {
        $sql = 'UPDATE SET id = source.id, name = source.name';
        $tokens = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create())->significantTokens();

        self::assertSame(['id' => 'source.id', 'name' => 'source.name'], (new \ZtdQuery\Platform\Postgres\Sql\Merge\ActionClause())->parseAssignments('MERGE INTO users u USING incoming s ON u.id = s.id WHEN MATCHED AND s.active THEN UPDATE SET name = s.name WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)', $sql, $tokens));
    }

    public function testParseInsert(): void
    {
        $sql = 'INSERT (id, name) VALUES (s.id, s.name)';
        $tokens = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create())->significantTokens();

        self::assertSame(['columns' => ['id', 'name'], 'values' => ['s.id', 's.name']], (new \ZtdQuery\Platform\Postgres\Sql\Merge\ActionClause())->parseInsert('MERGE INTO users u USING incoming s ON u.id = s.id WHEN MATCHED AND s.active THEN UPDATE SET name = s.name WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)', $sql, $tokens));

        $sql = 'INSERT DEFAULT VALUES';
        $tokens = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create())->significantTokens();

        self::assertSame(['columns' => [], 'values' => []], (new \ZtdQuery\Platform\Postgres\Sql\Merge\ActionClause())->parseInsert('MERGE INTO users u USING incoming s ON u.id = s.id WHEN MATCHED AND s.active THEN UPDATE SET name = s.name WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)', $sql, $tokens));
    }

    public function testParenthesizedList(): void
    {
        $sql = '(id, coalesce(name, \'x\'))';
        $tokens = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create())->significantTokens();

        self::assertSame(['items' => ['id', 'coalesce(name, \'x\')'], 'next' => 10], (new \ZtdQuery\Platform\Postgres\Sql\Merge\ActionClause())->parenthesizedList('MERGE INTO users u USING incoming s ON u.id = s.id WHEN MATCHED AND s.active THEN UPDATE SET name = s.name WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)', $sql, $tokens, 0));
    }
    public function testActionMatchesTheBranchKind(): void
    {
        $clause = (new \ZtdQuery\Platform\Postgres\Sql\Merge\ActionClause())->action('MERGE test', ' DELETE', \ZtdQuery\Platform\Postgres\Sql\Merge\PgSqlMergeMatchKind::Matched, 'active');
        self::assertSame(\ZtdQuery\Platform\Postgres\Sql\Merge\PgSqlMergeActionKind::Delete, $clause->actionKind);
        self::assertSame('active', $clause->conditionSql);
    }

    public function testInsertColumnsUnquotesOrderedIdentifiers(): void
    {
        self::assertSame(['id', 'Display Name'], (new \ZtdQuery\Platform\Postgres\Sql\Merge\ActionClause())->insertColumns('MERGE test', ['id', '"Display Name"']));
    }
}
