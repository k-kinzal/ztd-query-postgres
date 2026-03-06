<?php

declare(strict_types=1);

namespace Tests\Unit\Transformer;

use PHPUnit\Framework\TestCase;
use ZtdQuery\Exception\UnsupportedSqlException;
use ZtdQuery\Platform\Postgres\PgSqlParser;
use ZtdQuery\Platform\Postgres\Transformer\SelectTransformer;
use ZtdQuery\Platform\Postgres\Transformer\UpdateTransformer;
use ZtdQuery\Schema\ColumnType;
use ZtdQuery\Schema\ColumnTypeFamily;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;
use ZtdQuery\Platform\Postgres\PgSqlCastRenderer;
use ZtdQuery\Platform\Postgres\PgSqlIdentifierQuoter;

#[CoversClass(UpdateTransformer::class)]
#[UsesClass(PgSqlParser::class)]
#[UsesClass(SelectTransformer::class)]
#[UsesClass(PgSqlCastRenderer::class)]
#[UsesClass(PgSqlIdentifierQuoter::class)]
final class UpdateTransformerTest extends TestCase
{
    public function testBuildProjectionSimpleUpdate(): void
    {
        $transformer = new UpdateTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = "UPDATE users SET name = 'Bob' WHERE id = 1";
        $result = $transformer->buildProjection($sql, 'users', ['id', 'name']);

        self::assertSame('users', $result['table']);
        self::assertStringContainsString("'Bob' AS \"name\"", $result['sql']);
        self::assertStringContainsString('"users"."id"', $result['sql']);
        self::assertStringContainsString('FROM "users"', $result['sql']);
        self::assertStringContainsString('WHERE id = 1', $result['sql']);
    }

    public function testBuildProjectionWithAlias(): void
    {
        $transformer = new UpdateTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = "UPDATE users u SET name = 'Bob' WHERE id = 1";
        $result = $transformer->buildProjection($sql, 'users', ['id', 'name']);

        self::assertSame('users', $result['table']);
        self::assertStringContainsString('AS "u"', $result['sql']);
        self::assertStringContainsString('"u"."id"', $result['sql']);
    }

    public function testBuildProjectionWithMultipleSetColumns(): void
    {
        $transformer = new UpdateTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = "UPDATE users SET name = 'Bob', email = 'bob@example.com' WHERE id = 1";
        $result = $transformer->buildProjection($sql, 'users', ['id', 'name', 'email']);

        self::assertStringContainsString("'Bob' AS \"name\"", $result['sql']);
        self::assertStringContainsString("'bob@example.com' AS \"email\"", $result['sql']);
        self::assertStringContainsString('"users"."id"', $result['sql']);
    }

    public function testBuildProjectionWithFromClause(): void
    {
        $transformer = new UpdateTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = "UPDATE users SET name = orders.name FROM orders WHERE users.id = orders.user_id";
        $result = $transformer->buildProjection($sql, 'users', ['id', 'name']);

        self::assertStringContainsString('orders', $result['sql']);
        self::assertStringContainsString('WHERE users.id = orders.user_id', $result['sql']);
    }

    public function testBuildProjectionWithNoWhereClause(): void
    {
        $transformer = new UpdateTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = "UPDATE users SET active = true";
        $result = $transformer->buildProjection($sql, 'users', ['id', 'active']);

        self::assertStringContainsString('SELECT', $result['sql']);
        self::assertStringContainsString('FROM "users"', $result['sql']);
        self::assertStringNotContainsString('WHERE', $result['sql']);
    }

    public function testBuildProjectionWithEmptyColumnsUsesWildcard(): void
    {
        $transformer = new UpdateTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = "UPDATE users SET name = 'Bob' WHERE id = 1";
        $result = $transformer->buildProjection($sql, 'users', []);

        self::assertStringContainsString("'Bob' AS \"name\"", $result['sql']);
    }

    public function testBuildProjectionReturnsTablesArray(): void
    {
        $transformer = new UpdateTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = "UPDATE users SET name = 'Bob' WHERE id = 1";
        $result = $transformer->buildProjection($sql, 'users', ['id', 'name']);

        self::assertArrayHasKey('tables', $result);
        self::assertCount(1, $result['tables']);
        self::assertArrayHasKey('users', $result['tables']);
        self::assertSame('users', $result['tables']['users']['alias']);
    }

    public function testTransformAppliesCteShadowing(): void
    {
        $transformer = new UpdateTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = "UPDATE users SET name = 'Bob' WHERE id = 1";
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
        $transformer = new UpdateTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = 'UPDATE';
        $this->expectException(UnsupportedSqlException::class);
        $transformer->transform($sql, []);
    }

    public function testBuildProjectionUsesDoubleQuoteIdentifiers(): void
    {
        $transformer = new UpdateTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = "UPDATE users SET name = 'Bob' WHERE id = 1";
        $result = $transformer->buildProjection($sql, 'users', ['id', 'name']);

        self::assertStringContainsString('"users"', $result['sql']);
        self::assertStringContainsString('"id"', $result['sql']);
        self::assertStringContainsString('"name"', $result['sql']);
        self::assertStringNotContainsString('`', $result['sql']);
    }

    public function testBuildProjectionExactOutputSimple(): void
    {
        $transformer = new UpdateTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = "UPDATE users SET name = 'Bob' WHERE id = 1";
        $result = $transformer->buildProjection($sql, 'users', ['id', 'name']);

        self::assertSame(
            "SELECT 'Bob' AS \"name\", \"users\".\"id\" FROM \"users\" WHERE id = 1",
            $result['sql']
        );
    }

    public function testBuildProjectionExactOutputWithAlias(): void
    {
        $transformer = new UpdateTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = "UPDATE users u SET name = 'Bob' WHERE u.id = 1";
        $result = $transformer->buildProjection($sql, 'users', ['id', 'name']);

        self::assertSame(
            "SELECT 'Bob' AS \"name\", \"u\".\"id\" FROM \"users\" AS \"u\" WHERE u.id = 1",
            $result['sql']
        );
    }

    public function testBuildProjectionOnlySetColumnsWhenNoTableColumns(): void
    {
        $transformer = new UpdateTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = "UPDATE users SET name = 'Bob', email = 'b@x.com' WHERE id = 1";
        $result = $transformer->buildProjection($sql, 'users', []);

        self::assertSame(
            "SELECT 'Bob' AS \"name\", 'b@x.com' AS \"email\" FROM \"users\" WHERE id = 1",
            $result['sql']
        );
    }

    public function testBuildProjectionWildcardFallback(): void
    {
        $transformer = new UpdateTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = 'UPDATE users SET active = true';
        $result = $transformer->buildProjection($sql, 'users', []);

        self::assertSame(
            'SELECT true AS "active" FROM "users"',
            $result['sql']
        );
    }

    public function testBuildProjectionWithFromClauseExact(): void
    {
        $transformer = new UpdateTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = "UPDATE users SET name = orders.name FROM orders WHERE users.id = orders.user_id";
        $result = $transformer->buildProjection($sql, 'users', ['id', 'name']);

        self::assertSame(
            'SELECT orders.name AS "name", "users"."id" FROM "users", orders WHERE users.id = orders.user_id',
            $result['sql']
        );
    }

    public function testTransformWithEmptyTableContext(): void
    {
        $transformer = new UpdateTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = "UPDATE users SET name = 'Bob' WHERE id = 1";
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

    public function testBuildProjectionSetColumnExcludesFromRemainingColumns(): void
    {
        $transformer = new UpdateTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = "UPDATE users SET name = 'Bob', email = 'b@x.com' WHERE id = 1";
        $result = $transformer->buildProjection($sql, 'users', ['id', 'name', 'email']);

        self::assertSame(
            "SELECT 'Bob' AS \"name\", 'b@x.com' AS \"email\", \"users\".\"id\" FROM \"users\" WHERE id = 1",
            $result['sql']
        );
    }

    public function testBuildProjectionCoalesceColumnsFromTableContext(): void
    {
        $transformer = new UpdateTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = "UPDATE users SET name = 'Bob' WHERE id = 1";
        $tables = [
            'users' => [
                'rows' => [['id' => 1, 'name' => 'Alice', 'email' => 'a@b.com']],
                'columns' => ['id', 'name', 'email'],
                'columnTypes' => [
                    'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                    'name' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
                    'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
                ],
            ],
        ];

        $result = $transformer->transform($sql, $tables);
        self::assertStringContainsString("'Bob' AS \"name\"", $result);
        self::assertStringContainsString('"users"."id"', $result);
        self::assertStringContainsString('"users"."email"', $result);
    }

    public function testBuildProjectionSetColumnIsCoveredNotDuplicated(): void
    {
        $transformer = new UpdateTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = "UPDATE users SET name = 'Bob' WHERE id = 1";
        $result = $transformer->buildProjection($sql, 'users', ['id', 'name']);

        self::assertSame(1, substr_count($result['sql'], '"name"'));
    }
}
