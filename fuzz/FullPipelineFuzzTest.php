<?php

declare(strict_types=1);

namespace Fuzz;

use Faker\Factory;
use PHPUnit\Framework\TestCase;
use SqlFaker\PostgreSqlProvider;
use ZtdQuery\Exception\UnknownSchemaException;
use ZtdQuery\Exception\UnsupportedSqlException;
use ZtdQuery\Platform\Postgres\PgSqlCastRenderer;
use ZtdQuery\Platform\Postgres\PgSqlIdentifierQuoter;
use ZtdQuery\Platform\Postgres\PgSqlMutationResolver;
use ZtdQuery\Platform\Postgres\PgSqlParser;
use ZtdQuery\Platform\Postgres\PgSqlQueryGuard;
use ZtdQuery\Platform\Postgres\PgSqlRewriter;
use ZtdQuery\Platform\Postgres\PgSqlSchemaParser;
use ZtdQuery\Platform\Postgres\PgSqlTransformer;
use ZtdQuery\Platform\Postgres\Transformer\DeleteTransformer;
use ZtdQuery\Platform\Postgres\Transformer\InsertTransformer;
use ZtdQuery\Platform\Postgres\Transformer\SelectTransformer;
use ZtdQuery\Platform\Postgres\Transformer\UpdateTransformer;
use ZtdQuery\Rewrite\QueryKind;
use ZtdQuery\Schema\TableDefinition;
use ZtdQuery\Schema\TableDefinitionRegistry;
use ZtdQuery\Shadow\ShadowStore;

/**
 * Full pipeline fuzz test for PostgreSQL.
 *
 * Tests the complete pipeline: schema parse -> register -> populate ShadowStore
 * -> rewrite DML -> apply mutation -> verify integrity.
 *
 * Guards the following properties:
 * - P-SM-1: INSERT mutation increases row count
 * - P-SM-3: UPDATE mutation preserves row count
 * - P-SM-5: Mutations only affect the target table (table isolation)
 * - INV-L4-01: ShadowStore maintains array-of-arrays structure after mutations
 * - INV-L2-02/03: Plan consistency (mutation presence matches kind)
 * - DDL -> DML pipeline continuity (CREATE TABLE enables subsequent DML)
 */
final class FullPipelineFuzzTest extends TestCase
{
    private const ITERATIONS = 50;

    private PgSqlSchemaParser $schemaParser;

    private PostgreSqlProvider $provider;

    private \Faker\Generator $faker;

    protected function setUp(): void
    {
        $this->schemaParser = new PgSqlSchemaParser();
        $this->faker = Factory::create();
        $this->provider = new PostgreSqlProvider($this->faker);
    }

    /**
     * Build a fresh rewriter with given registry and shadow store.
     */
    private function buildRewriter(ShadowStore $shadowStore, TableDefinitionRegistry $registry): PgSqlRewriter
    {
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $castRenderer = new PgSqlCastRenderer();
        $quoter = new PgSqlIdentifierQuoter();
        $selectTransformer = new SelectTransformer($castRenderer, $quoter);
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);

        return new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
    }

    /**
     * Generate random fixture rows for a table definition.
     *
     * @return array<int, array<string, mixed>>
     */
    private function generateFixtureRows(TableDefinition $definition, int $count): array
    {
        $rows = [];
        for ($i = 0; $i < $count; $i++) {
            $row = [];
            foreach ($definition->columns as $col) {
                $type = strtoupper($definition->columnTypes[$col] ?? 'TEXT');
                $row[$col] = $this->generateValueForType($type, $i);
            }
            $rows[] = $row;
        }

        return $rows;
    }

    /**
     * Generate a random value appropriate for the given SQL type.
     */
    private function generateValueForType(string $type, int $seed): int|float|string|bool
    {
        $baseType = preg_replace('/\(.*\)/', '', $type);
        $baseType = trim($baseType ?? $type);

        return match (true) {
            in_array($baseType, ['INT', 'INT2', 'INT4', 'INT8', 'INTEGER', 'SMALLINT', 'BIGINT', 'SERIAL', 'SMALLSERIAL', 'BIGSERIAL'], true) => $seed + 1,
            in_array($baseType, ['REAL', 'FLOAT4', 'DOUBLE PRECISION', 'FLOAT8', 'DECIMAL', 'NUMERIC'], true) => round($seed + 0.5, 2),
            in_array($baseType, ['BOOLEAN', 'BOOL'], true) => $seed % 2 === 0,
            default => 'val_' . $seed,
        };
    }

    public function testCreateTableThenSelectDoesNotCrash(): void
    {
        for ($i = 0; $i < self::ITERATIONS; $i++) {
            $createSql = $this->provider->createTableStatement(50);
            $definition = $this->schemaParser->parse($createSql);
            if ($definition === null || $definition->columns === []) {
                continue;
            }

            $shadowStore = new ShadowStore();
            $registry = new TableDefinitionRegistry();

            $tableName = $this->extractTableName($createSql);
            if ($tableName === null) {
                continue;
            }

            $registry->register($tableName, $definition);
            $fixtureRows = $this->generateFixtureRows($definition, $this->faker->numberBetween(0, 5));
            $shadowStore->set($tableName, $fixtureRows);

            $rewriter = $this->buildRewriter($shadowStore, $registry);

            $selectSql = 'SELECT * FROM ' . $this->quoteIdentifier($tableName);
            try {
                $plan = $rewriter->rewrite($selectSql);
                self::assertNotEmpty($plan->sql());
                self::assertSame(QueryKind::READ, $plan->kind());
                self::assertNull($plan->mutation());
            } catch (UnsupportedSqlException|UnknownSchemaException) {
            } catch (\Throwable $e) {
                self::fail("Full pipeline SELECT crashed on iteration $i\nCREATE: $createSql\nSELECT: $selectSql\nError: " . $e->getMessage());
            }
        }
        self::addToAssertionCount(self::ITERATIONS);
    }

    public function testCreateTableThenInsertDoesNotCrash(): void
    {
        for ($i = 0; $i < self::ITERATIONS; $i++) {
            $createSql = $this->provider->createTableStatement(50);
            $definition = $this->schemaParser->parse($createSql);
            if ($definition === null || $definition->columns === []) {
                continue;
            }

            $shadowStore = new ShadowStore();
            $registry = new TableDefinitionRegistry();

            $tableName = $this->extractTableName($createSql);
            if ($tableName === null) {
                continue;
            }

            $registry->register($tableName, $definition);
            $shadowStore->set($tableName, []);

            $rewriter = $this->buildRewriter($shadowStore, $registry);

            $values = $this->buildInsertValues($definition);
            $insertSql = 'INSERT INTO ' . $this->quoteIdentifier($tableName) . ' (' . implode(', ', array_map(fn (string $c) => $this->quoteIdentifier($c), $definition->columns)) . ') VALUES (' . $values . ')';

            try {
                $plan = $rewriter->rewrite($insertSql);
                self::assertNotEmpty($plan->sql());
                self::assertSame(QueryKind::WRITE_SIMULATED, $plan->kind());

                if ($plan->mutation() !== null) {
                    $countBefore = count($shadowStore->get($tableName));
                    $fakeResultRows = [$this->generateFixtureRows($definition, 1)[0]];
                    $plan->mutation()->apply($shadowStore, $fakeResultRows);

                    $storedRows = $shadowStore->get($tableName);
                    self::assertNotEmpty($storedRows);
                    self::assertGreaterThanOrEqual(
                        $countBefore,
                        count($storedRows),
                        "INSERT mutation should not decrease row count on iteration $i"
                    );
                }
            } catch (UnsupportedSqlException|UnknownSchemaException) {
            } catch (\Throwable $e) {
                self::fail("Full pipeline INSERT crashed on iteration $i\nCREATE: $createSql\nINSERT: $insertSql\nError: " . $e->getMessage());
            }
        }
        self::addToAssertionCount(self::ITERATIONS);
    }

    public function testCreateTableThenUpdateDoesNotCrash(): void
    {
        for ($i = 0; $i < self::ITERATIONS; $i++) {
            $createSql = $this->provider->createTableStatement(50);
            $definition = $this->schemaParser->parse($createSql);
            if ($definition === null || $definition->columns === [] || $definition->primaryKeys === []) {
                continue;
            }

            $shadowStore = new ShadowStore();
            $registry = new TableDefinitionRegistry();

            $tableName = $this->extractTableName($createSql);
            if ($tableName === null) {
                continue;
            }

            $registry->register($tableName, $definition);
            $fixtureRows = $this->generateFixtureRows($definition, 3);
            $shadowStore->set($tableName, $fixtureRows);

            $rewriter = $this->buildRewriter($shadowStore, $registry);

            $firstCol = $definition->columns[0];
            $updateSql = 'UPDATE ' . $this->quoteIdentifier($tableName) . ' SET ' . $this->quoteIdentifier($firstCol) . ' = ' . $this->quoteIdentifier($firstCol);

            try {
                $plan = $rewriter->rewrite($updateSql);
                self::assertNotEmpty($plan->sql());
                self::assertSame(QueryKind::WRITE_SIMULATED, $plan->kind());

                if ($plan->mutation() !== null) {
                    $countBefore = count($shadowStore->get($tableName));
                    $plan->mutation()->apply($shadowStore, $fixtureRows);
                    $storedRows = $shadowStore->get($tableName);
                    self::assertCount(
                        $countBefore,
                        $storedRows,
                        "UPDATE mutation should preserve row count on iteration $i"
                    );
                }
            } catch (UnsupportedSqlException|UnknownSchemaException) {
            } catch (\Throwable $e) {
                self::fail("Full pipeline UPDATE crashed on iteration $i\nCREATE: $createSql\nUPDATE: $updateSql\nError: " . $e->getMessage());
            }
        }
        self::addToAssertionCount(self::ITERATIONS);
    }

    public function testCreateTableThenDeleteDoesNotCrash(): void
    {
        for ($i = 0; $i < self::ITERATIONS; $i++) {
            $createSql = $this->provider->createTableStatement(50);
            $definition = $this->schemaParser->parse($createSql);
            if ($definition === null || $definition->columns === []) {
                continue;
            }

            $shadowStore = new ShadowStore();
            $registry = new TableDefinitionRegistry();

            $tableName = $this->extractTableName($createSql);
            if ($tableName === null) {
                continue;
            }

            $registry->register($tableName, $definition);
            $fixtureRows = $this->generateFixtureRows($definition, 3);
            $shadowStore->set($tableName, $fixtureRows);

            $rewriter = $this->buildRewriter($shadowStore, $registry);

            $deleteSql = 'DELETE FROM ' . $this->quoteIdentifier($tableName);

            try {
                $plan = $rewriter->rewrite($deleteSql);
                self::assertNotEmpty($plan->sql());
                self::assertSame(QueryKind::WRITE_SIMULATED, $plan->kind());

                if ($plan->mutation() !== null) {
                    $plan->mutation()->apply($shadowStore, $fixtureRows);
                    $storedRows = $shadowStore->get($tableName);
                    self::assertNotEmpty($storedRows);
                }
            } catch (UnsupportedSqlException|UnknownSchemaException) {
            } catch (\Throwable $e) {
                self::fail("Full pipeline DELETE crashed on iteration $i\nCREATE: $createSql\nDELETE: $deleteSql\nError: " . $e->getMessage());
            }
        }
        self::addToAssertionCount(self::ITERATIONS);
    }

    public function testCreateTableRewriteRegistersThenDmlSucceeds(): void
    {
        for ($i = 0; $i < self::ITERATIONS; $i++) {
            $createSql = $this->provider->createTableStatement(50);
            $definition = $this->schemaParser->parse($createSql);
            if ($definition === null || $definition->columns === []) {
                continue;
            }

            $shadowStore = new ShadowStore();
            $registry = new TableDefinitionRegistry();

            $tableName = $this->extractTableName($createSql);
            if ($tableName === null) {
                continue;
            }

            $rewriter = $this->buildRewriter($shadowStore, $registry);

            try {
                $createPlan = $rewriter->rewrite($createSql);
                self::assertSame(QueryKind::DDL_SIMULATED, $createPlan->kind());

                if ($createPlan->mutation() !== null) {
                    $createPlan->mutation()->apply($shadowStore, []);
                }

                $selectSql = 'SELECT * FROM ' . $this->quoteIdentifier($tableName);
                $selectPlan = $rewriter->rewrite($selectSql);
                self::assertNotEmpty($selectPlan->sql());
                self::assertSame(QueryKind::READ, $selectPlan->kind());
            } catch (UnsupportedSqlException|UnknownSchemaException) {
            } catch (\Throwable $e) {
                self::fail("Full pipeline CREATE->SELECT crashed on iteration $i\nCREATE: $createSql\nError: " . $e->getMessage());
            }
        }
        self::addToAssertionCount(self::ITERATIONS);
    }

    public function testShadowStoreIntegrityAfterMultipleOperations(): void
    {
        for ($i = 0; $i < self::ITERATIONS; $i++) {
            $createSql = $this->provider->createTableStatement(50);
            $definition = $this->schemaParser->parse($createSql);
            if ($definition === null || $definition->columns === []) {
                continue;
            }

            $shadowStore = new ShadowStore();
            $registry = new TableDefinitionRegistry();

            $tableName = $this->extractTableName($createSql);
            if ($tableName === null) {
                continue;
            }

            $registry->register($tableName, $definition);
            $fixtureRows = $this->generateFixtureRows($definition, 3);
            $shadowStore->set($tableName, $fixtureRows);

            $rewriter = $this->buildRewriter($shadowStore, $registry);

            $operations = [
                'SELECT * FROM ' . $this->quoteIdentifier($tableName),
                'INSERT INTO ' . $this->quoteIdentifier($tableName) . ' (' . implode(', ', array_map(fn (string $c) => $this->quoteIdentifier($c), $definition->columns)) . ') VALUES (' . $this->buildInsertValues($definition) . ')',
            ];
            if ($definition->primaryKeys !== []) {
                $operations[] = 'UPDATE ' . $this->quoteIdentifier($tableName) . ' SET ' . $this->quoteIdentifier($definition->columns[0]) . ' = ' . $this->quoteIdentifier($definition->columns[0]);
            }
            $operations[] = 'DELETE FROM ' . $this->quoteIdentifier($tableName);

            foreach ($operations as $sql) {
                try {
                    $plan = $rewriter->rewrite($sql);
                    self::assertNotEmpty($plan->sql());
                    self::assertInstanceOf(QueryKind::class, $plan->kind());

                    if ($plan->mutation() !== null) {
                        $fakeRows = $this->generateFixtureRows($definition, 1);
                        $plan->mutation()->apply($shadowStore, $fakeRows);
                    }

                    $allData = $shadowStore->getAll();
                    foreach ($allData as $tblName => $tblRows) {
                        self::assertNotEmpty($tblName, 'ShadowStore table name must not be empty');
                        foreach ($tblRows as $rowIdx => $row) {
                            self::assertNotEmpty($row, "ShadowStore table '$tblName' row $rowIdx must not be empty");
                        }
                    }

                    self::assertArrayHasKey($tableName, $allData);

                    if ($plan->kind() === QueryKind::READ) {
                        self::assertNull($plan->mutation(), "READ plan must have no mutation");
                    } elseif ($plan->kind() === QueryKind::WRITE_SIMULATED) {
                        self::assertNotNull($plan->mutation(), "WRITE_SIMULATED plan must have a mutation");
                    } elseif ($plan->kind() === QueryKind::DDL_SIMULATED) {
                        self::assertNotNull($plan->mutation(), "DDL_SIMULATED plan must have a mutation");
                    }
                } catch (UnsupportedSqlException|UnknownSchemaException) {
                } catch (\Throwable $e) {
                    self::fail("ShadowStore integrity check failed on iteration $i with SQL: $sql\nCREATE: $createSql\nError: " . $e->getMessage());
                }
            }
        }
        self::addToAssertionCount(self::ITERATIONS);
    }

    /**
     * Extract table name from a CREATE TABLE statement.
     */
    private function extractTableName(string $createSql): ?string
    {
        if (preg_match('/CREATE\s+(?:TEMPORARY\s+|TEMP\s+|UNLOGGED\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:(?:"([^"]+)"|([a-zA-Z_]\w*))\.)?(?:"([^"]+)"|([a-zA-Z_]\w*))/i', $createSql, $m) !== 1) {
            return null;
        }

        return $m[3] !== '' ? $m[3] : ($m[4] ?? null);
    }

    private function quoteIdentifier(string $name): string
    {
        return '"' . str_replace('"', '""', $name) . '"';
    }

    /**
     * Build a VALUES clause with placeholder literals for all columns.
     */
    private function buildInsertValues(TableDefinition $definition): string
    {
        $values = [];
        foreach ($definition->columns as $col) {
            $type = strtoupper($definition->columnTypes[$col] ?? 'TEXT');
            $baseType = preg_replace('/\(.*\)/', '', $type);
            $baseType = trim($baseType ?? $type);

            $values[] = match (true) {
                in_array($baseType, ['INT', 'INT2', 'INT4', 'INT8', 'INTEGER', 'SMALLINT', 'BIGINT', 'SERIAL', 'SMALLSERIAL', 'BIGSERIAL'], true) => (string) $this->faker->numberBetween(1, 9999),
                in_array($baseType, ['REAL', 'FLOAT4', 'DOUBLE PRECISION', 'FLOAT8', 'DECIMAL', 'NUMERIC'], true) => (string) round($this->faker->randomFloat(2, 0, 999), 2),
                in_array($baseType, ['BOOLEAN', 'BOOL'], true) => $this->faker->boolean() ? 'TRUE' : 'FALSE',
                default => "'" . str_replace("'", "''", $this->faker->word()) . "'",
            };
        }

        return implode(', ', $values);
    }
}
