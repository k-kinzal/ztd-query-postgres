<?php

declare(strict_types=1);

namespace Fuzz;

use Faker\Factory;
use PHPUnit\Framework\TestCase;
use SqlFaker\PostgreSqlProvider;
use ZtdQuery\Platform\Postgres\PgSqlParser;
use ZtdQuery\Platform\Postgres\PgSqlQueryGuard;
use ZtdQuery\Rewrite\QueryKind;

/**
 * Fuzz tests for PgSqlQueryGuard::classify().
 *
 * Guards the following properties:
 * - INV-L1-01: classify() never throws on any input
 * - INV-L1-02: classify() is deterministic (same input -> same output)
 * - Kind correctness: SELECT->READ, INSERT/UPDATE/DELETE->WRITE_SIMULATED, DDL->DDL_SIMULATED
 */
final class ClassifyFuzzTest extends TestCase
{
    private const ITERATIONS = 100;

    private PgSqlQueryGuard $guard;

    private PostgreSqlProvider $provider;

    protected function setUp(): void
    {
        $this->guard = new PgSqlQueryGuard(new PgSqlParser());
        $faker = Factory::create();
        $this->provider = new PostgreSqlProvider($faker);
    }

    /**
     * INV-L1-01: classify() must never throw on any generated SQL.
     * INV-L1-02: classify() must be deterministic (same SQL -> same result).
     */
    public function testClassifyNeverThrowsAndIsDeterministic(): void
    {
        for ($i = 0; $i < self::ITERATIONS; $i++) {
            $sql = $this->provider->sql(maxDepth: 50);
            try {
                $result1 = $this->guard->classify($sql);
                $result2 = $this->guard->classify($sql);
                self::assertSame($result1, $result2, "classify() returned different results for the same SQL on iteration $i: $sql");
            } catch (\Throwable $e) {
                self::fail("classify() crashed on iteration $i with SQL: $sql\nError: " . $e->getMessage());
            }
        }
        self::addToAssertionCount(self::ITERATIONS);
    }

    public function testClassifySelectReturnsReadOrNull(): void
    {
        for ($i = 0; $i < self::ITERATIONS; $i++) {
            $sql = $this->provider->selectStatement(50);
            try {
                $result = $this->guard->classify($sql);
                if ($result !== null) {
                    self::assertSame(
                        QueryKind::READ,
                        $result,
                        "SELECT should classify as READ on iteration $i with SQL: $sql"
                    );
                }
            } catch (\Throwable $e) {
                self::fail("classify() crashed on SELECT iteration $i with SQL: $sql\nError: " . $e->getMessage());
            }
        }
        self::addToAssertionCount(self::ITERATIONS);
    }

    public function testClassifyInsertReturnsWriteSimulatedOrNull(): void
    {
        for ($i = 0; $i < self::ITERATIONS; $i++) {
            $sql = $this->provider->insertStatement(50);
            try {
                $result = $this->guard->classify($sql);
                if ($result !== null) {
                    self::assertSame(
                        QueryKind::WRITE_SIMULATED,
                        $result,
                        "INSERT should classify as WRITE_SIMULATED on iteration $i with SQL: $sql"
                    );
                }
            } catch (\Throwable $e) {
                self::fail("classify() crashed on INSERT iteration $i with SQL: $sql\nError: " . $e->getMessage());
            }
        }
        self::addToAssertionCount(self::ITERATIONS);
    }

    public function testClassifyUpdateReturnsWriteSimulatedOrNull(): void
    {
        for ($i = 0; $i < self::ITERATIONS; $i++) {
            $sql = $this->provider->updateStatement(50);
            try {
                $result = $this->guard->classify($sql);
                if ($result !== null) {
                    self::assertSame(
                        QueryKind::WRITE_SIMULATED,
                        $result,
                        "UPDATE should classify as WRITE_SIMULATED on iteration $i with SQL: $sql"
                    );
                }
            } catch (\Throwable $e) {
                self::fail("classify() crashed on UPDATE iteration $i with SQL: $sql\nError: " . $e->getMessage());
            }
        }
        self::addToAssertionCount(self::ITERATIONS);
    }

    public function testClassifyDeleteReturnsWriteSimulatedOrNull(): void
    {
        for ($i = 0; $i < self::ITERATIONS; $i++) {
            $sql = $this->provider->deleteStatement(50);
            try {
                $result = $this->guard->classify($sql);
                if ($result !== null) {
                    self::assertSame(
                        QueryKind::WRITE_SIMULATED,
                        $result,
                        "DELETE should classify as WRITE_SIMULATED on iteration $i with SQL: $sql"
                    );
                }
            } catch (\Throwable $e) {
                self::fail("classify() crashed on DELETE iteration $i with SQL: $sql\nError: " . $e->getMessage());
            }
        }
        self::addToAssertionCount(self::ITERATIONS);
    }

    public function testClassifyCreateTableReturnsDdlSimulatedOrNull(): void
    {
        for ($i = 0; $i < self::ITERATIONS; $i++) {
            $sql = $this->provider->createTableStatement(50);
            try {
                $result = $this->guard->classify($sql);
                if ($result !== null) {
                    self::assertSame(
                        QueryKind::DDL_SIMULATED,
                        $result,
                        "CREATE TABLE should classify as DDL_SIMULATED on iteration $i with SQL: $sql"
                    );
                }
            } catch (\Throwable $e) {
                self::fail("classify() crashed on CREATE TABLE iteration $i with SQL: $sql\nError: " . $e->getMessage());
            }
        }
        self::addToAssertionCount(self::ITERATIONS);
    }

    public function testClassifyDropTableReturnsDdlSimulatedOrNull(): void
    {
        for ($i = 0; $i < self::ITERATIONS; $i++) {
            $sql = $this->provider->dropTableStatement(50);
            try {
                $result = $this->guard->classify($sql);
                if ($result !== null) {
                    self::assertSame(
                        QueryKind::DDL_SIMULATED,
                        $result,
                        "DROP TABLE should classify as DDL_SIMULATED on iteration $i with SQL: $sql"
                    );
                }
            } catch (\Throwable $e) {
                self::fail("classify() crashed on DROP TABLE iteration $i with SQL: $sql\nError: " . $e->getMessage());
            }
        }
        self::addToAssertionCount(self::ITERATIONS);
    }

    public function testClassifyAlterTableReturnsDdlSimulatedOrNull(): void
    {
        for ($i = 0; $i < self::ITERATIONS; $i++) {
            $sql = $this->provider->alterTableStatement(50);
            try {
                $result = $this->guard->classify($sql);
                if ($result !== null) {
                    self::assertSame(
                        QueryKind::DDL_SIMULATED,
                        $result,
                        "ALTER TABLE should classify as DDL_SIMULATED on iteration $i with SQL: $sql"
                    );
                }
            } catch (\Throwable $e) {
                self::fail("classify() crashed on ALTER TABLE iteration $i with SQL: $sql\nError: " . $e->getMessage());
            }
        }
        self::addToAssertionCount(self::ITERATIONS);
    }
}
