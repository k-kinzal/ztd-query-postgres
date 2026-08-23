<?php

declare(strict_types=1);

namespace Tests\Unit;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Platform\Postgres\PgSqlReadOnlyDiagnosticStatement;

#[CoversClass(PgSqlReadOnlyDiagnosticStatement::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlLexerProfile::class)]
final class PgSqlReadOnlyDiagnosticStatementTest extends TestCase
{
    #[DataProvider('providerStatement')]
    public function testClassifiesOnlySafePostgreSqlDiagnostics(string $sql, bool $expected): void
    {
        self::assertSame($expected, PgSqlReadOnlyDiagnosticStatement::isSafe($sql));
    }

    /** @return iterable<string, array{string, bool}> */
    public static function providerStatement(): iterable
    {
        yield 'explain select' => ['EXPLAIN SELECT * FROM users', true];
        yield 'non-executing update' => ['EXPLAIN UPDATE users SET active = FALSE', true];
        yield 'analyze select' => ['EXPLAIN (ANALYZE TRUE, FORMAT JSON) SELECT * FROM users', true];
        yield 'analyse select' => ['EXPLAIN ANALYSE SELECT * FROM users', true];
        yield 'executing update' => ['EXPLAIN ANALYZE UPDATE users SET active = FALSE', false];
        yield 'data-modifying cte' => ['EXPLAIN ANALYZE WITH d AS (DELETE FROM users RETURNING id) SELECT * FROM d', false];
        yield 'show setting' => ['SHOW server_version', true];
        yield 'show without setting' => ['SHOW', false];
        yield 'mysql describe' => ['DESCRIBE users', false];
        yield 'multiple statements' => ['EXPLAIN SELECT 1; DELETE FROM users', false];
        yield 'empty explain' => ['EXPLAIN', false];
    }
}
