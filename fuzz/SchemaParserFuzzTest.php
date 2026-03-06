<?php

declare(strict_types=1);

namespace Fuzz;

use Faker\Factory;
use PHPUnit\Framework\TestCase;
use SqlFaker\PostgreSqlProvider;
use ZtdQuery\Platform\Postgres\PgSqlSchemaParser;
use ZtdQuery\Schema\TableDefinition;

/**
 * Fuzz tests for PgSqlSchemaParser::parse().
 *
 * Guards the following properties:
 * - P-SP-0: parse() never crashes on any generated CREATE TABLE
 * - P-SP-1: primaryKeys is a subset of columns
 * - P-SP-2: columnTypes keys are a subset of columns
 * - P-SP-3: notNullColumns is a subset of columns
 * - P-SP-4: uniqueConstraints columns are subsets of columns
 * - P-SP-5: typedColumns keys are a subset of columns
 * - P-SP-6: Non-null result always has non-empty columns
 * - P-SP-7: parse() returns null for non-CREATE TABLE statements
 */
final class SchemaParserFuzzTest extends TestCase
{
    private const ITERATIONS = 100;

    private PgSqlSchemaParser $parser;

    private PostgreSqlProvider $provider;

    protected function setUp(): void
    {
        $this->parser = new PgSqlSchemaParser();
        $faker = Factory::create();
        $this->provider = new PostgreSqlProvider($faker);
    }

    public function testParseDoesNotCrashOnRandomCreateTable(): void
    {
        for ($i = 0; $i < self::ITERATIONS; $i++) {
            $sql = $this->provider->createTableStatement(50);
            try {
                $result = $this->parser->parse($sql);
                if ($result !== null) {
                    self::assertInstanceOf(TableDefinition::class, $result);
                }
            } catch (\Throwable $e) {
                self::fail("parse() crashed on iteration $i with SQL: $sql\nError: " . $e->getMessage());
            }
        }
        self::addToAssertionCount(self::ITERATIONS);
    }

    public function testParseStructuralInvariantsOnRandomCreateTable(): void
    {
        for ($i = 0; $i < self::ITERATIONS; $i++) {
            $sql = $this->provider->createTableStatement(50);
            try {
                $result = $this->parser->parse($sql);
                if ($result === null) {
                    continue;
                }

                foreach ($result->primaryKeys as $pk) {
                    self::assertContains(
                        $pk,
                        $result->columns,
                        "Primary key '$pk' is not in columns on iteration $i with SQL: $sql",
                    );
                }

                foreach (array_keys($result->columnTypes) as $colName) {
                    self::assertContains(
                        $colName,
                        $result->columns,
                        "Column type key '$colName' is not in columns on iteration $i with SQL: $sql",
                    );
                }

                foreach ($result->notNullColumns as $notNull) {
                    self::assertContains(
                        $notNull,
                        $result->columns,
                        "Not-null column '$notNull' is not in columns on iteration $i with SQL: $sql",
                    );
                }

                foreach ($result->uniqueConstraints as $constraintName => $constraintCols) {
                    foreach ($constraintCols as $col) {
                        self::assertContains(
                            $col,
                            $result->columns,
                            "Unique constraint '$constraintName' column '$col' is not in columns on iteration $i with SQL: $sql",
                        );
                    }
                }

                self::assertNotEmpty($result->columns, "columns is empty for non-null result on iteration $i with SQL: $sql");

                foreach (array_keys($result->typedColumns) as $typedCol) {
                    self::assertContains(
                        $typedCol,
                        $result->columns,
                        "typedColumns key '$typedCol' is not in columns on iteration $i with SQL: $sql",
                    );
                }
            } catch (\Throwable $e) {
                self::fail("parse() crashed on iteration $i with SQL: $sql\nError: " . $e->getMessage());
            }
        }
        self::addToAssertionCount(self::ITERATIONS);
    }

    public function testParseReturnsNullOnNonCreateTableSql(): void
    {
        for ($i = 0; $i < self::ITERATIONS; $i++) {
            $sql = $this->provider->selectStatement(50);
            try {
                $result = $this->parser->parse($sql);
                self::assertNull($result, "parse() should return null for SELECT on iteration $i with SQL: $sql");
            } catch (\Throwable $e) {
                self::fail("parse() crashed on SELECT iteration $i with SQL: $sql\nError: " . $e->getMessage());
            }
        }
        self::addToAssertionCount(self::ITERATIONS);
    }
}
