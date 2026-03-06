<?php

declare(strict_types=1);

namespace Tests\Unit\Transformer;

use PHPUnit\Framework\TestCase;
use ZtdQuery\Exception\UnsupportedSqlException;
use ZtdQuery\Platform\Postgres\PgSqlParser;
use ZtdQuery\Platform\Postgres\Transformer\DeleteTransformer;
use ZtdQuery\Platform\Postgres\Transformer\SelectTransformer;
use ZtdQuery\Schema\ColumnType;
use ZtdQuery\Schema\ColumnTypeFamily;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;
use ZtdQuery\Platform\Postgres\PgSqlCastRenderer;
use ZtdQuery\Platform\Postgres\PgSqlIdentifierQuoter;

#[CoversClass(DeleteTransformer::class)]
#[UsesClass(PgSqlParser::class)]
#[UsesClass(SelectTransformer::class)]
#[UsesClass(PgSqlCastRenderer::class)]
#[UsesClass(PgSqlIdentifierQuoter::class)]
final class DeleteTransformerTest extends TestCase
{
    public function testBuildProjectionSimpleDelete(): void
    {
        $transformer = new DeleteTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = 'DELETE FROM users WHERE id = 1';
        $result = $transformer->buildProjection($sql, 'users', ['id', 'name']);

        self::assertSame('users', $result['table']);
        self::assertStringContainsString('"users"."id" AS "id"', $result['sql']);
        self::assertStringContainsString('"users"."name" AS "name"', $result['sql']);
        self::assertStringContainsString('FROM "users"', $result['sql']);
        self::assertStringContainsString('WHERE id = 1', $result['sql']);
    }

    public function testBuildProjectionWithAlias(): void
    {
        $transformer = new DeleteTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = 'DELETE FROM users u WHERE u.id = 1';
        $result = $transformer->buildProjection($sql, 'users', ['id', 'name']);

        self::assertSame('users', $result['table']);
        self::assertStringContainsString('AS "u"', $result['sql']);
        self::assertStringContainsString('"u"."id" AS "id"', $result['sql']);
        self::assertStringContainsString('"u"."name" AS "name"', $result['sql']);
    }

    public function testBuildProjectionWithUsingClause(): void
    {
        $transformer = new DeleteTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = 'DELETE FROM users USING orders WHERE users.id = orders.user_id AND orders.status = \'canceled\'';
        $result = $transformer->buildProjection($sql, 'users', ['id', 'name']);

        self::assertStringContainsString('orders', $result['sql']);
        self::assertStringContainsString('WHERE', $result['sql']);
    }

    public function testBuildProjectionWithNoWhereClause(): void
    {
        $transformer = new DeleteTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = 'DELETE FROM users';
        $result = $transformer->buildProjection($sql, 'users', ['id', 'name']);

        self::assertStringContainsString('SELECT', $result['sql']);
        self::assertStringContainsString('FROM "users"', $result['sql']);
        self::assertStringNotContainsString('WHERE', $result['sql']);
    }

    public function testBuildProjectionWithEmptyColumnsUsesWildcard(): void
    {
        $transformer = new DeleteTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = 'DELETE FROM users WHERE id = 1';
        $result = $transformer->buildProjection($sql, 'users', []);

        self::assertStringContainsString('"users".*', $result['sql']);
    }

    public function testBuildProjectionReturnsTablesArray(): void
    {
        $transformer = new DeleteTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = 'DELETE FROM users WHERE id = 1';
        $result = $transformer->buildProjection($sql, 'users', ['id']);

        self::assertArrayHasKey('tables', $result);
        self::assertCount(1, $result['tables']);
        self::assertArrayHasKey('users', $result['tables']);
        self::assertSame('users', $result['tables']['users']['alias']);
    }

    public function testBuildProjectionWithAliasReturnsAliasInTables(): void
    {
        $transformer = new DeleteTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = 'DELETE FROM users u WHERE u.id = 1';
        $result = $transformer->buildProjection($sql, 'users', ['id']);

        self::assertArrayHasKey('users', $result['tables']);
        self::assertSame('u', $result['tables']['users']['alias']);
    }

    public function testTransformAppliesCteShadowing(): void
    {
        $transformer = new DeleteTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = 'DELETE FROM users WHERE id = 1';
        $tables = [
            'users' => [
                'rows' => [['id' => 1, 'name' => 'Alice']],
                'columns' => ['id', 'name'],
                'columnTypes' => [
                    'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                    'name' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
                ],
            ],
        ];

        $result = $transformer->transform($sql, $tables);
        self::assertStringContainsString('WITH', $result);
        self::assertStringContainsString('"users" AS MATERIALIZED', $result);
    }

    public function testTransformThrowsOnUnresolvableTarget(): void
    {
        $transformer = new DeleteTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = 'DELETE FROM';
        $this->expectException(UnsupportedSqlException::class);
        $transformer->transform($sql, []);
    }

    public function testBuildProjectionUsesDoubleQuoteIdentifiers(): void
    {
        $transformer = new DeleteTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = 'DELETE FROM users WHERE id = 1';
        $result = $transformer->buildProjection($sql, 'users', ['id', 'name']);

        self::assertStringContainsString('"users"', $result['sql']);
        self::assertStringContainsString('"id"', $result['sql']);
        self::assertStringContainsString('"name"', $result['sql']);
        self::assertStringNotContainsString('`', $result['sql']);
    }

    public function testBuildProjectionExactOutputSimple(): void
    {
        $transformer = new DeleteTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = 'DELETE FROM users WHERE id = 1';
        $result = $transformer->buildProjection($sql, 'users', ['id', 'name']);

        self::assertSame(
            'SELECT "users"."id" AS "id", "users"."name" AS "name" FROM "users" WHERE id = 1',
            $result['sql']
        );
    }

    public function testBuildProjectionExactOutputWithAlias(): void
    {
        $transformer = new DeleteTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = 'DELETE FROM users u WHERE u.id = 1';
        $result = $transformer->buildProjection($sql, 'users', ['id', 'name']);

        self::assertSame(
            'SELECT "u"."id" AS "id", "u"."name" AS "name" FROM "users" AS "u" WHERE u.id = 1',
            $result['sql']
        );
    }

    public function testBuildProjectionExactOutputWithUsing(): void
    {
        $transformer = new DeleteTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = "DELETE FROM users USING orders WHERE users.id = orders.user_id AND orders.status = 'canceled'";
        $result = $transformer->buildProjection($sql, 'users', ['id', 'name']);

        self::assertSame(
            "SELECT \"users\".\"id\" AS \"id\", \"users\".\"name\" AS \"name\" FROM \"users\", orders WHERE users.id = orders.user_id AND orders.status = 'canceled'",
            $result['sql']
        );
    }

    public function testBuildProjectionEmptyColumnsWildcard(): void
    {
        $transformer = new DeleteTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = 'DELETE FROM users WHERE id = 1';
        $result = $transformer->buildProjection($sql, 'users', []);

        self::assertSame(
            'SELECT "users".* FROM "users" WHERE id = 1',
            $result['sql']
        );
    }

    public function testBuildProjectionNoWhere(): void
    {
        $transformer = new DeleteTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = 'DELETE FROM users';
        $result = $transformer->buildProjection($sql, 'users', ['id']);

        self::assertSame(
            'SELECT "users"."id" AS "id" FROM "users"',
            $result['sql']
        );
    }

    public function testTransformWithEmptyTableContext(): void
    {
        $transformer = new DeleteTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = 'DELETE FROM users WHERE id = 1';
        $tables = [
            'users' => [
                'rows' => [],
                'columns' => ['id', 'name'],
                'columnTypes' => [
                    'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                    'name' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
                ],
            ],
        ];

        $result = $transformer->transform($sql, $tables);
        self::assertStringContainsString('WHERE FALSE', $result);
        self::assertStringContainsString('"users" AS MATERIALIZED', $result);
    }

    public function testTransformCoalesceUsesTableContextColumns(): void
    {
        $transformer = new DeleteTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = 'DELETE FROM users WHERE id = 1';
        $tables = [
            'users' => [
                'rows' => [['id' => 1, 'name' => 'Alice']],
                'columns' => ['id', 'name'],
                'columnTypes' => [
                    'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                    'name' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
                ],
            ],
        ];

        $result = $transformer->transform($sql, $tables);
        self::assertStringContainsString('"users"."id" AS "id"', $result);
        self::assertStringContainsString('"users"."name" AS "name"', $result);
    }

    public function testTransformWithoutColumnsInContextUsesWildcard(): void
    {
        $transformer = new DeleteTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = 'DELETE FROM users WHERE id = 1';
        $tables = [];

        $result = $transformer->transform($sql, $tables);
        self::assertStringContainsString('"users".*', $result);
    }
}
