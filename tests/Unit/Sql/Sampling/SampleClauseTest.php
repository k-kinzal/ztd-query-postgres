<?php

declare(strict_types=1);

namespace Tests\Unit\Sql\Sampling;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Sql\Sampling\SampleClause::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Sampling\SampleTokens::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Sampling\PgSqlTableSample::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Sampling\PgSqlTableSampleMethod::class)]
final class SampleClauseTest extends TestCase
{
    public function testParseSample(): void
    {
        $sql = 'users TABLESAMPLE SYSTEM (10) REPEATABLE (7)';
        $tokens = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create())->significantTokens();
        $reference = ['name' => 'users', 'start' => 0, 'unqualifiedStart' => 0, 'end' => 5];

        self::assertEquals(new \ZtdQuery\Platform\Postgres\Sql\Sampling\PgSqlTableSample(tableName: 'users', sourceSql: 'users', aliasSql: '', method: \ZtdQuery\Platform\Postgres\Sql\Sampling\PgSqlTableSampleMethod::System, percentageSql: '10', seedSql: '7', startOffset: 0, endOffset: 44), (new \ZtdQuery\Platform\Postgres\Sql\Sampling\SampleClause())->parseSample($sql, $tokens, $reference, 1, $tokens[0]));
    }
    public function testRepeatablePreservesTheSeedAndClosingOffset(): void
    {
        $sql = 'users TABLESAMPLE SYSTEM (10) REPEATABLE (7)';
        $tokens = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create())->significantTokens();
        self::assertSame(['seed' => '7', 'end' => strlen($sql)], (new \ZtdQuery\Platform\Postgres\Sql\Sampling\SampleClause())->repeatable($sql, $tokens, $tokens[0], 5));
    }

    public function testAliasSqlPreservesTheRelationAlias(): void
    {
        $sql = 'users u TABLESAMPLE SYSTEM (10)';
        $tokens = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create())->significantTokens();
        self::assertSame('u', (new \ZtdQuery\Platform\Postgres\Sql\Sampling\SampleClause())->aliasSql($sql, $tokens, ['name' => 'users', 'start' => 0, 'unqualifiedStart' => 0, 'end' => 5], $tokens[0], $tokens[2]));
    }
}
