<?php

declare(strict_types=1);

namespace Tests\Unit\Transformer;

use PHPUnit\Framework\TestCase;
use ZtdQuery\Exception\UnsupportedSqlException;
use ZtdQuery\Platform\Postgres\PgSqlParser;
use ZtdQuery\Platform\Postgres\Transformer\InsertTransformer;
use ZtdQuery\Platform\Postgres\Transformer\SelectTransformer;
use ZtdQuery\Schema\ColumnType;
use ZtdQuery\Schema\ColumnTypeFamily;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;
use ZtdQuery\Platform\Postgres\PgSqlCastRenderer;
use ZtdQuery\Platform\Postgres\PgSqlIdentifierQuoter;

#[CoversClass(InsertTransformer::class)]
#[UsesClass(PgSqlParser::class)]
#[UsesClass(SelectTransformer::class)]
#[UsesClass(PgSqlCastRenderer::class)]
#[UsesClass(PgSqlIdentifierQuoter::class)]
final class InsertTransformerTest extends TestCase
{
    public function testInsertValuesWithExplicitColumns(): void
    {
        $transformer = new InsertTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = "INSERT INTO users (id, name) VALUES (1, 'Alice')";
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
        self::assertStringContainsString('SELECT', $result);
        self::assertStringContainsString('"id"', $result);
        self::assertStringContainsString('"name"', $result);
    }

    public function testInsertValuesWithoutColumnsUsesTableContext(): void
    {
        $transformer = new InsertTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = "INSERT INTO users VALUES (1, 'Bob')";
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
        self::assertStringContainsString('SELECT', $result);
        self::assertStringContainsString('"id"', $result);
        self::assertStringContainsString('"name"', $result);
    }

    public function testInsertMultipleRows(): void
    {
        $transformer = new InsertTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = "INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')";
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
        self::assertStringContainsString('SELECT', $result);
        self::assertStringContainsString('UNION ALL', $result);
    }

    public function testInsertSelect(): void
    {
        $transformer = new InsertTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = 'INSERT INTO archive (id, name) SELECT id, name FROM users WHERE active = false';
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
        self::assertStringContainsString('SELECT', $result);
        self::assertStringContainsString('"users"', $result);
        self::assertStringContainsString('AS MATERIALIZED', $result);
    }

    public function testInsertWithoutTableThrowsException(): void
    {
        $transformer = new InsertTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = 'INSERT INTO';
        $this->expectException(UnsupportedSqlException::class);
        $transformer->transform($sql, []);
    }

    public function testInsertWithoutColumnsAndNoTableContextThrowsException(): void
    {
        $transformer = new InsertTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = "INSERT INTO unknown_table VALUES (1, 'test')";
        $this->expectException(UnsupportedSqlException::class);
        $this->expectExceptionMessage('Cannot determine columns');
        $transformer->transform($sql, []);
    }

    public function testInsertValueCountMismatchThrowsException(): void
    {
        $transformer = new InsertTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = "INSERT INTO users (id, name, email) VALUES (1, 'Alice')";
        $tables = [
            'users' => [
                'rows' => [],
                'columns' => ['id', 'name', 'email'],
                'columnTypes' => [],
            ],
        ];

        $this->expectException(UnsupportedSqlException::class);
        $this->expectExceptionMessage('values count does not match');
        $transformer->transform($sql, $tables);
    }

    public function testInsertAppliesCteShadowing(): void
    {
        $transformer = new InsertTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = "INSERT INTO users (id, name) VALUES (2, 'Bob')";
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
        self::assertStringContainsString('SELECT', $result);
    }

    public function testInsertWithSchemaQualifiedTable(): void
    {
        $transformer = new InsertTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = 'INSERT INTO public.users (id, name) VALUES (1, \'Test\')';
        $tables = [];

        $result = $transformer->transform($sql, $tables);
        self::assertStringContainsString('SELECT', $result);
    }

    public function testInsertValuesWithWhitespaceAroundValues(): void
    {
        $transformer = new InsertTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = "INSERT INTO users (id, name) VALUES ( 1 , 'Alice' )";
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
        self::assertStringContainsString('1 AS "id"', $result);
        self::assertStringNotContainsString(' 1  AS', $result);
    }

    public function testInsertExactOutputFormat(): void
    {
        $transformer = new InsertTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = "INSERT INTO users (id, name) VALUES (1, 'Alice')";
        $tables = [];

        $result = $transformer->transform($sql, $tables);
        self::assertStringContainsString('SELECT 1 AS "id"', $result);
    }

    public function testInsertMultiRowExactFormat(): void
    {
        $transformer = new InsertTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = "INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')";
        $tables = [];

        $result = $transformer->transform($sql, $tables);
        self::assertSame(
            "SELECT 1 AS \"id\", 'Alice' AS \"name\" UNION ALL SELECT 2 AS \"id\", 'Bob' AS \"name\"",
            $result
        );
    }

    public function testInsertValuesColumnExprAppearsInOutput(): void
    {
        $transformer = new InsertTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = "INSERT INTO users (id, name) VALUES (1, 'Alice')";
        $tables = [];

        $result = $transformer->transform($sql, $tables);
        self::assertStringContainsString("'Alice' AS \"name\"", $result);
        self::assertStringContainsString('1 AS "id"', $result);
    }

    public function testInsertNoValuesThrows(): void
    {
        $transformer = new InsertTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = 'INSERT INTO users (id)';
        $tables = [];

        $this->expectException(UnsupportedSqlException::class);
        $transformer->transform($sql, $tables);
    }
}
