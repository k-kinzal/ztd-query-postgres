<?php

declare(strict_types=1);

namespace Tests\Unit\Transformer;

use Tests\Contract\TransformerContractTest;
use ZtdQuery\Platform\Postgres\Transformer\SelectTransformer;
use ZtdQuery\Rewrite\SqlTransformer;
use ZtdQuery\Schema\ColumnType;
use ZtdQuery\Schema\ColumnTypeFamily;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;
use ZtdQuery\Platform\Postgres\PgSqlCastRenderer;
use ZtdQuery\Platform\Postgres\PgSqlIdentifierQuoter;

#[CoversClass(SelectTransformer::class)]
#[UsesClass(PgSqlCastRenderer::class)]
#[UsesClass(PgSqlIdentifierQuoter::class)]
final class SelectTransformerTest extends TransformerContractTest
{
    protected function createTransformer(): SqlTransformer
    {
        return new SelectTransformer();
    }

    protected function selectSql(): string
    {
        return 'SELECT * FROM users WHERE id = 1';
    }

    #[\Override]
    protected function nativeStringType(): string
    {
        return 'TEXT';
    }

    public function testTransformWithNoTablesReturnsOriginal(): void
    {
        $transformer = new SelectTransformer();
        $sql = 'SELECT 1';
        self::assertSame($sql, $transformer->transform($sql, []));
    }

    public function testTransformWithEmptyShadowGeneratesEmptyCte(): void
    {
        $transformer = new SelectTransformer();
        $tables = [
            'users' => [
                'rows' => [],
                'columns' => ['id', 'name'],
                'columnTypes' => ['id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'), 'name' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT')],
            ],
        ];

        $result = $transformer->transform('SELECT * FROM users', $tables);
        self::assertStringContainsString('WHERE FALSE', $result);
        self::assertStringContainsString('AS MATERIALIZED', $result);
        self::assertStringContainsString('"users"', $result);
        self::assertStringContainsString('CAST(NULL AS INTEGER)', $result);
        self::assertStringContainsString('CAST(NULL AS TEXT)', $result);
    }

    public function testTransformWithSingleRowUsesSingleSelect(): void
    {
        $transformer = new SelectTransformer();
        $tables = [
            'users' => [
                'rows' => [['id' => 1, 'name' => 'Alice']],
                'columns' => ['id', 'name'],
                'columnTypes' => ['id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'), 'name' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT')],
            ],
        ];

        $result = $transformer->transform('SELECT * FROM users', $tables);
        self::assertStringContainsString('AS MATERIALIZED', $result);
        self::assertStringNotContainsString('VALUES', $result);
        self::assertStringContainsString('"id"', $result);
        self::assertStringContainsString('"name"', $result);
    }

    public function testTransformWithMultiRowUsesValuesClause(): void
    {
        $transformer = new SelectTransformer();
        $tables = [
            'users' => [
                'rows' => [
                    ['id' => 1, 'name' => 'Alice'],
                    ['id' => 2, 'name' => 'Bob'],
                ],
                'columns' => ['id', 'name'],
                'columnTypes' => ['id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'), 'name' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT')],
            ],
        ];

        $result = $transformer->transform('SELECT * FROM users', $tables);
        self::assertStringContainsString('VALUES', $result);
        self::assertStringContainsString('AS MATERIALIZED', $result);
    }

    public function testTransformUsesDoubleQuoteIdentifiers(): void
    {
        $transformer = new SelectTransformer();
        $tables = [
            'users' => [
                'rows' => [['id' => 1]],
                'columns' => ['id'],
                'columnTypes' => ['id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER')],
            ],
        ];

        $result = $transformer->transform('SELECT * FROM users', $tables);
        self::assertStringContainsString('"users"', $result);
        self::assertStringContainsString('"id"', $result);
        self::assertStringNotContainsString('`', $result);
    }

    public function testTransformDoesNotShadowUnreferencedTables(): void
    {
        $transformer = new SelectTransformer();
        $tables = [
            'users' => [
                'rows' => [['id' => 1]],
                'columns' => ['id'],
                'columnTypes' => ['id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER')],
            ],
            'orders' => [
                'rows' => [['id' => 1]],
                'columns' => ['id'],
                'columnTypes' => ['id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER')],
            ],
        ];

        $result = $transformer->transform('SELECT * FROM users', $tables);
        self::assertStringContainsString('"users"', $result);
        self::assertStringNotContainsString('"orders"', $result);
    }

    public function testTransformPreservesExistingWithClause(): void
    {
        $transformer = new SelectTransformer();
        $tables = [
            'users' => [
                'rows' => [['id' => 1, 'name' => 'Alice']],
                'columns' => ['id', 'name'],
                'columnTypes' => ['id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'), 'name' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT')],
            ],
        ];

        $sql = 'WITH cte AS (SELECT 1) SELECT * FROM users, cte';
        $result = $transformer->transform($sql, $tables);
        self::assertStringContainsString('WITH', $result);
        self::assertStringContainsString('"users" AS MATERIALIZED', $result);
    }

    public function testTransformUsesPostgresqlCastTypes(): void
    {
        $transformer = new SelectTransformer();
        $tables = [
            'test' => [
                'rows' => [],
                'columns' => ['a', 'b', 'c', 'd', 'e'],
                'columnTypes' => [
                    'a' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                    'b' => new ColumnType(ColumnTypeFamily::BOOLEAN, 'BOOLEAN'),
                    'c' => new ColumnType(ColumnTypeFamily::TIMESTAMP, 'TIMESTAMP'),
                    'd' => new ColumnType(ColumnTypeFamily::JSON, 'JSONB'),
                    'e' => new ColumnType(ColumnTypeFamily::BINARY, 'BYTEA'),
                ],
            ],
        ];

        $result = $transformer->transform('SELECT * FROM test', $tables);
        self::assertStringContainsString('CAST(NULL AS INTEGER)', $result);
        self::assertStringContainsString('CAST(NULL AS BOOLEAN)', $result);
        self::assertStringContainsString('CAST(NULL AS TIMESTAMP)', $result);
        self::assertStringContainsString('CAST(NULL AS JSONB)', $result);
        self::assertStringContainsString('CAST(NULL AS BYTEA)', $result);
    }

    public function testTransformNullValues(): void
    {
        $transformer = new SelectTransformer();
        $tables = [
            'users' => [
                'rows' => [['id' => 1, 'name' => null]],
                'columns' => ['id', 'name'],
                'columnTypes' => ['id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'), 'name' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT')],
            ],
        ];

        $result = $transformer->transform('SELECT * FROM users', $tables);
        self::assertStringContainsString('NULL', $result);
        self::assertStringNotContainsString("'NULL'", $result);
    }

    public function testTransformSingleQuoteInData(): void
    {
        $transformer = new SelectTransformer();
        $tables = [
            'users' => [
                'rows' => [['id' => 1, 'name' => "O'Brien"]],
                'columns' => ['id', 'name'],
                'columnTypes' => ['id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'), 'name' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT')],
            ],
        ];

        $result = $transformer->transform('SELECT * FROM users', $tables);
        self::assertStringContainsString("O''Brien", $result);
    }

    public function testTransformEmptyStringData(): void
    {
        $transformer = new SelectTransformer();
        $tables = [
            'users' => [
                'rows' => [['id' => 1, 'name' => '']],
                'columns' => ['id', 'name'],
                'columnTypes' => ['id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'), 'name' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT')],
            ],
        ];

        $result = $transformer->transform('SELECT * FROM users', $tables);
        self::assertStringContainsString("''", $result);
    }

    public function testTransformBooleanValues(): void
    {
        $transformer = new SelectTransformer();
        $tables = [
            'flags' => [
                'rows' => [['id' => 1, 'active' => true], ['id' => 2, 'active' => false]],
                'columns' => ['id', 'active'],
                'columnTypes' => ['id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'), 'active' => new ColumnType(ColumnTypeFamily::BOOLEAN, 'BOOLEAN')],
            ],
        ];

        $result = $transformer->transform('SELECT * FROM flags', $tables);
        self::assertStringContainsString('VALUES', $result);
        self::assertStringContainsString('"flags"', $result);
    }

    public function testTransformAllNullRow(): void
    {
        $transformer = new SelectTransformer();
        $tables = [
            'users' => [
                'rows' => [['id' => null, 'name' => null]],
                'columns' => ['id', 'name'],
                'columnTypes' => ['id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'), 'name' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT')],
            ],
        ];

        $result = $transformer->transform('SELECT * FROM users', $tables);
        self::assertStringContainsString('"users"', $result);
        self::assertStringContainsString('AS MATERIALIZED', $result);
    }

    public function testTransformWithNoFromDual(): void
    {
        $transformer = new SelectTransformer();
        $tables = [
            'users' => [
                'rows' => [],
                'columns' => ['id'],
                'columnTypes' => ['id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER')],
            ],
        ];

        $result = $transformer->transform('SELECT * FROM users', $tables);
        self::assertStringNotContainsString('DUAL', $result);
        self::assertStringContainsString('WHERE FALSE', $result);
    }

    public function testTransformExactOutputEmptyRows(): void
    {
        $transformer = new SelectTransformer();
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

        $result = $transformer->transform('SELECT * FROM users', $tables);
        self::assertSame(
            'WITH "users" AS MATERIALIZED (SELECT CAST(NULL AS INTEGER) AS "id", CAST(NULL AS TEXT) AS "name" WHERE FALSE)' . "\n" . 'SELECT * FROM users',
            $result
        );
    }

    public function testTransformExactOutputSingleRow(): void
    {
        $transformer = new SelectTransformer();
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

        $result = $transformer->transform('SELECT * FROM users', $tables);
        self::assertSame(
            "WITH \"users\" AS MATERIALIZED (SELECT CAST('1' AS INTEGER) AS \"id\", CAST('Alice' AS TEXT) AS \"name\")\nSELECT * FROM users",
            $result
        );
    }

    public function testTransformExactOutputMultiRow(): void
    {
        $transformer = new SelectTransformer();
        $tables = [
            'users' => [
                'rows' => [
                    ['id' => 1, 'name' => 'Alice'],
                    ['id' => 2, 'name' => 'Bob'],
                ],
                'columns' => ['id', 'name'],
                'columnTypes' => [
                    'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                    'name' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
                ],
            ],
        ];

        $result = $transformer->transform('SELECT * FROM users', $tables);
        $expected = "WITH \"users\" AS MATERIALIZED (\n"
            . "  SELECT * FROM (VALUES\n"
            . "    (CAST('1' AS INTEGER), CAST('Alice' AS TEXT)),\n"
            . "    (CAST('2' AS INTEGER), CAST('Bob' AS TEXT))\n"
            . "  ) AS t(\"id\", \"name\")\n"
            . ")\nSELECT * FROM users";
        self::assertSame($expected, $result);
    }

    public function testTransformExactOutputWithExistingWith(): void
    {
        $transformer = new SelectTransformer();
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

        $sql = 'WITH cte AS (SELECT 1) SELECT * FROM users, cte';
        $result = $transformer->transform($sql, $tables);
        self::assertSame(
            "WITH \"users\" AS MATERIALIZED (SELECT CAST('1' AS INTEGER) AS \"id\", CAST('Alice' AS TEXT) AS \"name\"),\n cte AS (SELECT 1) SELECT * FROM users, cte",
            $result
        );
    }

    public function testTransformWithCustomCastRendererAndQuoter(): void
    {
        $castRenderer = new PgSqlCastRenderer();
        $quoter = new PgSqlIdentifierQuoter();
        $transformer = new SelectTransformer($castRenderer, $quoter);
        $tables = [
            'users' => [
                'rows' => [['id' => 1]],
                'columns' => ['id'],
                'columnTypes' => ['id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER')],
            ],
        ];

        $result = $transformer->transform('SELECT * FROM users', $tables);
        self::assertSame(
            "WITH \"users\" AS MATERIALIZED (SELECT CAST('1' AS INTEGER) AS \"id\")\nSELECT * FROM users",
            $result
        );
    }

    public function testTransformTableNotReferencedInSql(): void
    {
        $transformer = new SelectTransformer();
        $tables = [
            'users' => [
                'rows' => [['id' => 1]],
                'columns' => ['id'],
                'columnTypes' => ['id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER')],
            ],
        ];

        $result = $transformer->transform('SELECT 1', $tables);
        self::assertSame('SELECT 1', $result);
    }

    public function testTransformEmptyColumnsAndEmptyRowsSkipsTable(): void
    {
        $transformer = new SelectTransformer();
        $tables = [
            'users' => [
                'rows' => [],
                'columns' => [],
                'columnTypes' => [],
            ],
        ];

        $result = $transformer->transform('SELECT * FROM users', $tables);
        self::assertSame('SELECT * FROM users', $result);
    }

    public function testTransformEmptyColumnsWithRowsDerivesCols(): void
    {
        $transformer = new SelectTransformer();
        $tables = [
            'users' => [
                'rows' => [['id' => 1, 'name' => 'Alice']],
                'columns' => [],
                'columnTypes' => [],
            ],
        ];

        $result = $transformer->transform('SELECT * FROM users', $tables);
        self::assertStringContainsString('"users" AS MATERIALIZED', $result);
        self::assertStringContainsString('"id"', $result);
        self::assertStringContainsString('"name"', $result);
    }

    public function testTransformEmptyColumnsMultiRowsDerivesCols(): void
    {
        $transformer = new SelectTransformer();
        $tables = [
            'users' => [
                'rows' => [
                    ['id' => 1, 'name' => 'Alice'],
                    ['id' => 2, 'name' => 'Bob', 'extra' => 'x'],
                ],
                'columns' => [],
                'columnTypes' => [],
            ],
        ];

        $result = $transformer->transform('SELECT * FROM users', $tables);
        self::assertStringContainsString('"users" AS MATERIALIZED', $result);
        self::assertStringContainsString('"id"', $result);
        self::assertStringContainsString('"name"', $result);
        self::assertStringContainsString('"extra"', $result);
    }

    public function testTransformWithIntValueNoColumnType(): void
    {
        $transformer = new SelectTransformer();
        $tables = [
            'users' => [
                'rows' => [['id' => 42]],
                'columns' => ['id'],
                'columnTypes' => [],
            ],
        ];

        $result = $transformer->transform('SELECT * FROM users', $tables);
        self::assertStringContainsString('CAST(42 AS INTEGER)', $result);
    }

    public function testTransformWithStringValueNoColumnType(): void
    {
        $transformer = new SelectTransformer();
        $tables = [
            'users' => [
                'rows' => [['name' => 'Alice']],
                'columns' => ['name'],
                'columnTypes' => [],
            ],
        ];

        $result = $transformer->transform('SELECT * FROM users', $tables);
        self::assertStringContainsString("CAST('Alice' AS TEXT)", $result);
    }

    public function testTransformWithBoolValueNoColumnType(): void
    {
        $transformer = new SelectTransformer();
        $tables = [
            'users' => [
                'rows' => [['active' => true], ['active' => false]],
                'columns' => ['active'],
                'columnTypes' => [],
            ],
        ];

        $result = $transformer->transform('SELECT * FROM users', $tables);
        self::assertStringContainsString('TRUE', $result);
        self::assertStringContainsString('FALSE', $result);
    }

    public function testTransformWithFloatValueNoColumnType(): void
    {
        $transformer = new SelectTransformer();
        $tables = [
            'users' => [
                'rows' => [['score' => 3.14]],
                'columns' => ['score'],
                'columnTypes' => [],
            ],
        ];

        $result = $transformer->transform('SELECT * FROM users', $tables);
        self::assertStringContainsString('3.14', $result);
    }

    public function testTransformWithBoolValueWithColumnType(): void
    {
        $transformer = new SelectTransformer();
        $tables = [
            'users' => [
                'rows' => [['active' => true]],
                'columns' => ['active'],
                'columnTypes' => ['active' => new ColumnType(ColumnTypeFamily::BOOLEAN, 'BOOLEAN')],
            ],
        ];

        $result = $transformer->transform('SELECT * FROM users', $tables);
        self::assertStringContainsString("CAST('1' AS BOOLEAN)", $result);
    }

    public function testTransformWithFloatValueWithColumnType(): void
    {
        $transformer = new SelectTransformer();
        $tables = [
            'users' => [
                'rows' => [['score' => 9.5]],
                'columns' => ['score'],
                'columnTypes' => ['score' => new ColumnType(ColumnTypeFamily::DECIMAL, 'NUMERIC')],
            ],
        ];

        $result = $transformer->transform('SELECT * FROM users', $tables);
        self::assertStringContainsString("CAST('9.5' AS NUMERIC)", $result);
    }

    public function testTransformWithUnknownColumnTypeUsesTextFallback(): void
    {
        $transformer = new SelectTransformer();
        $tables = [
            'users' => [
                'rows' => [],
                'columns' => ['data'],
                'columnTypes' => [],
            ],
        ];

        $result = $transformer->transform('SELECT * FROM users', $tables);
        self::assertStringContainsString('CAST(NULL AS TEXT)', $result);
    }

    public function testTransformWithMultipleTablesOnlyReferencedAreIncluded(): void
    {
        $transformer = new SelectTransformer();
        $tables = [
            'users' => [
                'rows' => [['id' => 1]],
                'columns' => ['id'],
                'columnTypes' => ['id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER')],
            ],
            'orders' => [
                'rows' => [['id' => 10]],
                'columns' => ['id'],
                'columnTypes' => ['id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER')],
            ],
        ];

        $result = $transformer->transform('SELECT * FROM users JOIN orders ON users.id = orders.id', $tables);
        self::assertStringContainsString('"users" AS MATERIALIZED', $result);
        self::assertStringContainsString('"orders" AS MATERIALIZED', $result);
    }

    public function testTransformContinueVsBreakMultipleTables(): void
    {
        $transformer = new SelectTransformer();
        $tables = [
            'not_referenced' => [
                'rows' => [['id' => 1]],
                'columns' => ['id'],
                'columnTypes' => ['id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER')],
            ],
            'users' => [
                'rows' => [['id' => 2]],
                'columns' => ['id'],
                'columnTypes' => ['id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER')],
            ],
        ];

        $result = $transformer->transform('SELECT * FROM users', $tables);
        self::assertStringContainsString('"users" AS MATERIALIZED', $result);
        self::assertStringNotContainsString('"not_referenced"', $result);
    }

    public function testTransformUnsupportedValueTypeThrows(): void
    {
        $transformer = new SelectTransformer();
        $tables = [
            'users' => [
                'rows' => [['data' => [1, 2, 3]]],
                'columns' => ['data'],
                'columnTypes' => [],
            ],
        ];

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Unsupported value type for CTE shadowing');
        $transformer->transform('SELECT * FROM users', $tables);
    }

    public function testTransformObjectWithToStringNoColumnType(): void
    {
        $transformer = new SelectTransformer();
        $obj = new class () {
            public function __toString(): string
            {
                return 'stringified';
            }
        };
        $tables = [
            'users' => [
                'rows' => [['val' => $obj]],
                'columns' => ['val'],
                'columnTypes' => [],
            ],
        ];

        $result = $transformer->transform('SELECT * FROM users', $tables);
        self::assertStringContainsString('stringified', $result);
    }

    public function testTransformSerializesObjectWithColumnType(): void
    {
        $transformer = new SelectTransformer();
        $obj = new \stdClass();
        $tables = [
            'users' => [
                'rows' => [['val' => $obj]],
                'columns' => ['val'],
                'columnTypes' => ['val' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT')],
            ],
        ];

        $result = $transformer->transform('SELECT * FROM users', $tables);
        self::assertStringContainsString('CAST(', $result);
    }

    public function testTransformWithLeadingCommentAndWith(): void
    {
        $transformer = new SelectTransformer();
        $tables = [
            'users' => [
                'rows' => [['id' => 1]],
                'columns' => ['id'],
                'columnTypes' => ['id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER')],
            ],
        ];

        $sql = "/* comment */WITH cte AS (SELECT 1) SELECT * FROM users, cte";
        $result = $transformer->transform($sql, $tables);
        self::assertStringStartsWith('/* comment */WITH "users"', $result);
    }

    public function testTransformBoolTrueAndFalseDistinct(): void
    {
        $transformer = new SelectTransformer();
        $tables = [
            'flags' => [
                'rows' => [['active' => true]],
                'columns' => ['active'],
                'columnTypes' => [],
            ],
        ];

        $resultTrue = $transformer->transform('SELECT * FROM flags', $tables);
        self::assertStringContainsString('TRUE', $resultTrue);
        self::assertStringNotContainsString('FALSE', $resultTrue);

        $tables['flags']['rows'] = [['active' => false]];
        $resultFalse = $transformer->transform('SELECT * FROM flags', $tables);
        self::assertStringContainsString('FALSE', $resultFalse);
        self::assertStringNotContainsString('TRUE', $resultFalse);
    }

    public function testTransformEmptyColumnsAndEmptyRowsSkipsButContinuesToNext(): void
    {
        $transformer = new SelectTransformer();
        $tables = [
            'empty_table' => [
                'rows' => [],
                'columns' => [],
                'columnTypes' => [],
            ],
            'users' => [
                'rows' => [['id' => 1]],
                'columns' => ['id'],
                'columnTypes' => ['id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER')],
            ],
        ];

        $result = $transformer->transform('SELECT * FROM empty_table, users', $tables);
        self::assertStringContainsString('"users" AS MATERIALIZED', $result);
        self::assertStringNotContainsString('"empty_table" AS MATERIALIZED', $result);
    }

    public function testTransformPregReplaceCountOnlyOnce(): void
    {
        $transformer = new SelectTransformer();
        $tables = [
            'users' => [
                'rows' => [['id' => 1]],
                'columns' => ['id'],
                'columnTypes' => ['id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER')],
            ],
        ];

        $sql = 'WITH a AS (SELECT 1) SELECT * FROM users WHERE id IN (WITH b AS (SELECT 2) SELECT * FROM b)';
        $result = $transformer->transform($sql, $tables);
        $withCount = substr_count($result, 'WITH');
        self::assertSame(2, $withCount);
    }

    public function testTransformEmptyColumnsWithMultipleRowsDifferentKeys(): void
    {
        $transformer = new SelectTransformer();
        $tables = [
            'data' => [
                'rows' => [
                    ['a' => 1],
                    ['a' => 2, 'b' => 3],
                ],
                'columns' => [],
                'columnTypes' => [],
            ],
        ];

        $result = $transformer->transform('SELECT * FROM data', $tables);
        self::assertStringContainsString('"a"', $result);
        self::assertStringContainsString('"b"', $result);
    }

    public function testTransformEmptyColumnsDerivesColumnsNotIncludingDuplicates(): void
    {
        $transformer = new SelectTransformer();
        $tables = [
            'data' => [
                'rows' => [
                    ['x' => 1, 'y' => 2],
                    ['x' => 3, 'y' => 4],
                ],
                'columns' => [],
                'columnTypes' => [],
            ],
        ];

        $result = $transformer->transform('SELECT * FROM data', $tables);
        self::assertSame(1, substr_count($result, '"x"'));
    }
}
