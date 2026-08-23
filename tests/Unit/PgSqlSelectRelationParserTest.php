<?php

declare(strict_types=1);

namespace Tests\Unit;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Platform\Postgres\PgSqlLexerProfile;
use ZtdQuery\Platform\Postgres\PgSqlSelectRelationParser;

#[CoversClass(PgSqlSelectRelationParser::class)]
#[UsesClass(PgSqlLexerProfile::class)]
final class PgSqlSelectRelationParserTest extends TestCase
{
    public function testFindsPostgreSqlRelationsWithoutTableFunctions(): void
    {
        $sql = 'SELECT * FROM generate_series(1, 3) day LEFT JOIN LATERAL (SELECT * FROM ONLY public."orders") totals ON TRUE JOIN audit_log a ON TRUE';

        self::assertSame(
            ['audit_log', 'orders'],
            (new PgSqlSelectRelationParser())->tableNames($sql),
        );
    }

    public function testFromClausesUsePostgreSqlBoundaries(): void
    {
        $parser = new PgSqlSelectRelationParser();

        self::assertSame(
            ['users', 'archived'],
            $parser->fromClauses('SELECT * FROM users FETCH FIRST 1 ROW; SELECT * FROM archived FOR UPDATE'),
        );
    }

    public function testReferencesPreserveOffsetsAndUnqualifyOnlyTargets(): void
    {
        $parser = new PgSqlSelectRelationParser();
        $sql = 'SELECT * FROM public."users" u JOIN logs.audit_log a ON TRUE';

        self::assertSame(['users', 'audit_log'], array_column($parser->references($sql), 'name'));
        self::assertSame(
            'SELECT * FROM "users" u JOIN logs.audit_log a ON TRUE',
            $parser->unqualify($sql, ['users']),
        );
    }

    public function testReferencesExposeExactQualifiedIdentifierOffsets(): void
    {
        $parser = new PgSqlSelectRelationParser();
        $sql = 'SELECT * FROM public."a""b" a JOIN users u ON TRUE JOIN logs.orders o ON TRUE';

        self::assertSame([
            [
                'name' => 'a"b',
                'start' => 14,
                'unqualifiedStart' => 21,
                'end' => 27,
            ],
            [
                'name' => 'users',
                'start' => 35,
                'unqualifiedStart' => 35,
                'end' => 40,
            ],
            [
                'name' => 'orders',
                'start' => 56,
                'unqualifiedStart' => 61,
                'end' => 67,
            ],
        ], $parser->references($sql));
    }

    public function testUnqualifiesMultipleTargetsCaseInsensitivelyFromRightToLeft(): void
    {
        $sql = 'SELECT * FROM users u JOIN public.Users u2 ON TRUE JOIN logs.orders o ON TRUE';

        self::assertSame(
            'SELECT * FROM users u JOIN Users u2 ON TRUE JOIN orders o ON TRUE',
            (new PgSqlSelectRelationParser())->unqualify($sql, ['USERS', 'ORDERS']),
        );
    }

    public function testKeepsOneCaseInsensitiveRelationName(): void
    {
        self::assertSame(
            ['Users'],
            (new PgSqlSelectRelationParser())->tableNames('SELECT * FROM Users JOIN users ON TRUE'),
        );
    }

    public function testSetOperatorsAndRepeatedFromCloseTheCurrentSelectScope(): void
    {
        $parser = new PgSqlSelectRelationParser();
        $sql = 'FROM orphan; SELECT 1 UNION FROM union_hidden; SELECT 1 INTERSECT FROM intersect_hidden; SELECT 1 EXCEPT FROM except_hidden; SELECT * FROM visible';

        self::assertSame(['visible'], $parser->fromClauses($sql));
        self::assertSame(['users'], $parser->tableNames('SELECT * FROM users FROM ignored'));
        self::assertSame(['users FROM ignored'], $parser->fromClauses('SELECT * FROM users FROM ignored'));
        self::assertSame([], $parser->tableNames('FROM orphan'));
    }

    public function testFindsParenthesizedJoinedRelationsButNotDerivedProducers(): void
    {
        $parser = new PgSqlSelectRelationParser();
        $groupedSql = 'SELECT * FROM (public.users JOIN logs.orders ON TRUE) grouped, extra';

        self::assertSame(['users', 'orders', 'extra'], $parser->tableNames($groupedSql));
        self::assertSame([
            ['name' => 'users', 'start' => 15, 'unqualifiedStart' => 22, 'end' => 27],
            ['name' => 'orders', 'start' => 33, 'unqualifiedStart' => 38, 'end' => 44],
            ['name' => 'extra', 'start' => 63, 'unqualifiedStart' => 63, 'end' => 68],
        ], $parser->references($groupedSql));
        self::assertSame([], $parser->tableNames('SELECT * FROM () empty_group'));
        self::assertSame(['extra'], $parser->tableNames('SELECT * FROM (), extra'));
        self::assertSame([], $parser->tableNames('SELECT * FROM (VALUES rogue, leaked) produced'));
        self::assertSame([], $parser->tableNames('SELECT * FROM (WITH rogue, leaked) produced'));
        self::assertSame(
            ['users'],
            $parser->tableNames('SELECT * FROM (SELECT id, name FROM public.users) produced'),
        );
        self::assertSame(['users', 'orders'], $parser->tableNames('SELECT * FROM users, orders'));
    }

    public function testNestedSelectFromClausesStopAtTheirOwnScopeBoundary(): void
    {
        $parser = new PgSqlSelectRelationParser();

        self::assertSame(
            ['(SELECT * FROM inner_table) derived', 'inner_table'],
            $parser->fromClauses('SELECT * FROM (SELECT * FROM inner_table) derived WHERE active = 1'),
        );
        self::assertSame(
            ['(SELECT * FROM inner_table WHERE id = 1) derived', 'inner_table'],
            $parser->fromClauses(
                'SELECT * FROM (SELECT * FROM inner_table WHERE id = 1) derived WHERE active = 1',
            ),
        );
    }

    public function testReferencesDoNotReadPastTheFromClauseBoundary(): void
    {
        self::assertSame(
            ['users'],
            (new PgSqlSelectRelationParser())->tableNames('SELECT * FROM users WHERE TRUE JOIN leaked ON TRUE'),
        );
    }

    public function testParenthesizedRelationOffsetsDoNotIncludeFollowingAlias(): void
    {
        $sql = 'SELECT * FROM          (public.users) grouped, extra';

        self::assertSame([
            ['name' => 'users', 'start' => 24, 'unqualifiedStart' => 31, 'end' => 36],
            ['name' => 'extra', 'start' => 47, 'unqualifiedStart' => 47, 'end' => 52],
        ], (new PgSqlSelectRelationParser())->references($sql));
    }

    public function testDoesNotTreatQuotedFunctionNameAsRelation(): void
    {
        self::assertSame(
            [],
            (new PgSqlSelectRelationParser())->tableNames('SELECT * FROM "generate_series"(1, 3) day'),
        );
    }

    /** @param list<string> $expected */
    #[DataProvider('providerPostgresFromTerminators')]
    public function testStopsFromClauseAtEveryPostgresBoundary(string $suffix, array $expected): void
    {
        self::assertSame(
            $expected,
            (new PgSqlSelectRelationParser())->fromClauses('SELECT * FROM users ' . $suffix),
        );
    }

    /** @return iterable<string, array{string, list<string>}> */
    public static function providerPostgresFromTerminators(): iterable
    {
        yield 'where' => ['WHERE active = TRUE', ['users']];
        yield 'group by' => ['GROUP BY id', ['users']];
        yield 'having' => ['HAVING count(*) > 1', ['users']];
        yield 'window' => ['WINDOW w AS ()', ['users']];
        yield 'order by' => ['ORDER BY id', ['users']];
        yield 'limit' => ['LIMIT 1', ['users']];
        yield 'offset' => ['OFFSET 1', ['users']];
        yield 'fetch' => ['FETCH FIRST 1 ROW', ['users']];
        yield 'for' => ['FOR UPDATE', ['users']];
        yield 'returning' => ['RETURNING id', ['users']];
        yield 'union' => ['UNION SELECT * FROM archived', ['users', 'archived']];
        yield 'intersect' => ['INTERSECT SELECT * FROM archived', ['users', 'archived']];
        yield 'except' => ['EXCEPT SELECT * FROM archived', ['users', 'archived']];
    }

    public function testRejectsMySqlAndLiteralSourceForms(): void
    {
        $parser = new PgSqlSelectRelationParser();

        self::assertSame([], $parser->tableNames('SELECT * FROM `users`'));
        self::assertSame([], $parser->tableNames("SELECT * FROM 'literal'"));
        self::assertSame([], $parser->tableNames('SELECT * FROM VALUES (1)'));
        self::assertSame([], $parser->tableNames('SELECT * FROM ""'));
        self::assertSame([], $parser->tableNames('SELECT * FROM "unterminated'));
        self::assertSame([], $parser->tableNames('SELECT * FROM SELECT'));
        self::assertSame([], $parser->tableNames('SELECT * FROM WITH'));
    }
}
