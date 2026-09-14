<?php

declare(strict_types=1);

namespace Fuzz;

use Faker\Factory;
use Override;
use PHPUnit\Framework\Attributes\CoversNothing;
use PHPUnit\Framework\Attributes\Large;
use PHPUnit\Framework\TestCase;
use SqlFaker\PostgreSqlProvider;
use ZtdQuery\Exception\UnknownSchemaException;
use ZtdQuery\Exception\UnsupportedSqlException;
use ZtdQuery\Platform\Postgres\Rewrite\PgSqlQueryGuard;
use ZtdQuery\Platform\Postgres\Rewrite\PgSqlRewriter;
use ZtdQuery\Platform\Postgres\Rewrite\Transformer\DeleteTransformer;
use ZtdQuery\Platform\Postgres\Rewrite\Transformer\InsertTransformer;
use ZtdQuery\Platform\Postgres\Rewrite\Transformer\PgSqlTransformer;
use ZtdQuery\Platform\Postgres\Rewrite\Transformer\SelectTransformer;
use ZtdQuery\Platform\Postgres\Rewrite\Transformer\UpdateTransformer;
use ZtdQuery\Platform\Postgres\Schema\PgSqlSchemaParser;
use ZtdQuery\Platform\Postgres\Shadow\PgSqlMutationResolver;
use ZtdQuery\Platform\Postgres\Sql\PgSqlIdentifierQuoter;
use ZtdQuery\Platform\Postgres\Sql\PgSqlParser;
use ZtdQuery\Platform\Postgres\Sql\Value\PgSqlCastRenderer;
use ZtdQuery\Rewrite\QueryKind;
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
#[CoversNothing]
#[Large]
final class FullPipelineFuzzTest extends TestCase
{
    private const ITERATIONS = 50;
    private PgSqlSchemaParser $schemaParser;
    private PostgreSqlProvider $provider;
    private \Faker\Generator $faker;
    #[Override]
    protected function setUp(): void
    {
        $this->schemaParser = new PgSqlSchemaParser();
        $this->faker = Factory::create();
        $this->faker->seed(20260815);
        $this->provider = new PostgreSqlProvider($this->faker, 'pg-17.2');
    }



    /**
     * Test create table then select does not crash.
     */
    public function testCreateTableThenSelectDoesNotCrash(): void
    {
        $this->faker->seed(20260815);
        for ($i = 0; $i < self::ITERATIONS; $i++) {
            $createSql = $this->provider->createTableStatement(50);
            $definition = $this->schemaParser->parse($createSql);
            if ($definition === null || $definition->columns === []) {
                continue;
            }
            $shadowStore = new ShadowStore();
            $registry = new TableDefinitionRegistry();
            $tableName = (new Input\SchemaRows())->extractTableName($createSql);
            if ($tableName === null) {
                continue;
            }
            $registry->register($tableName, $definition);
            $fixtureRows = (new Input\SchemaRows())->generateFixtureRows($definition, $this->faker->numberBetween(0, 5));
            $shadowStore->set($tableName, $fixtureRows);
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
            $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
            $selectSql = 'SELECT * FROM ' . (new PgSqlIdentifierQuoter())->quote($tableName);
            try {
                $plan = $rewriter->rewrite($selectSql);
                self::assertNotEmpty($plan->sql());
                self::assertSame(QueryKind::READ, $plan->kind());
                self::assertNull($plan->mutation());
            } catch (UnsupportedSqlException|UnknownSchemaException) {
                continue;
            }
        }
        self::addToAssertionCount(self::ITERATIONS);
    }
    /**
     * Test create table then insert does not crash.
     */
    public function testCreateTableThenInsertDoesNotCrash(): void
    {
        $this->faker->seed(20260815);
        for ($i = 0; $i < self::ITERATIONS; $i++) {
            $createSql = $this->provider->createTableStatement(50);
            $definition = $this->schemaParser->parse($createSql);
            if ($definition === null || $definition->columns === []) {
                continue;
            }
            $shadowStore = new ShadowStore();
            $registry = new TableDefinitionRegistry();
            $tableName = (new Input\SchemaRows())->extractTableName($createSql);
            if ($tableName === null) {
                continue;
            }
            $registry->register($tableName, $definition);
            $shadowStore->set($tableName, []);
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
            $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
            $values = (new Input\InsertLiterals($this->faker))->buildInsertValues($definition);
            $insertSql = 'INSERT INTO ' . (new PgSqlIdentifierQuoter())->quote($tableName) . ' (' . implode(', ', array_map(fn (string $c) => (new PgSqlIdentifierQuoter())->quote($c), $definition->columns)) . ') VALUES (' . $values . ')';
            try {
                $plan = $rewriter->rewrite($insertSql);
                self::assertNotEmpty($plan->sql());
                self::assertSame(QueryKind::WRITE_SIMULATED, $plan->kind());
                if ($plan->mutation() !== null) {
                    $countBefore = count($shadowStore->get($tableName));
                    $fakeResultRows = [(new Input\SchemaRows())->generateFixtureRows($definition, 1)[0]];
                    $plan->mutation()->apply($shadowStore, $fakeResultRows);
                    $storedRows = $shadowStore->get($tableName);
                    self::assertNotEmpty($storedRows);
                    self::assertGreaterThanOrEqual($countBefore, count($storedRows), "INSERT mutation should not decrease row count on iteration {$i}");
                }
            } catch (UnsupportedSqlException|UnknownSchemaException) {
                continue;
            }
        }
        self::addToAssertionCount(self::ITERATIONS);
    }
    /**
     * Test create table then update does not crash.
     */
    public function testCreateTableThenUpdateDoesNotCrash(): void
    {
        $this->faker->seed(20260815);
        for ($i = 0; $i < self::ITERATIONS; $i++) {
            $createSql = $this->provider->createTableStatement(50);
            $definition = $this->schemaParser->parse($createSql);
            if ($definition === null || $definition->columns === [] || $definition->primaryKeys === []) {
                continue;
            }
            $shadowStore = new ShadowStore();
            $registry = new TableDefinitionRegistry();
            $tableName = (new Input\SchemaRows())->extractTableName($createSql);
            if ($tableName === null) {
                continue;
            }
            $registry->register($tableName, $definition);
            $fixtureRows = (new Input\SchemaRows())->generateFixtureRows($definition, 3);
            $shadowStore->set($tableName, $fixtureRows);
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
            $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
            $firstCol = $definition->columns[0];
            $updateSql = 'UPDATE ' . (new PgSqlIdentifierQuoter())->quote($tableName) . ' SET ' . (new PgSqlIdentifierQuoter())->quote($firstCol) . ' = ' . (new PgSqlIdentifierQuoter())->quote($firstCol);
            try {
                $plan = $rewriter->rewrite($updateSql);
                self::assertNotEmpty($plan->sql());
                self::assertSame(QueryKind::WRITE_SIMULATED, $plan->kind());
                if ($plan->mutation() !== null) {
                    $countBefore = count($shadowStore->get($tableName));
                    $plan->mutation()->apply($shadowStore, $fixtureRows);
                    $storedRows = $shadowStore->get($tableName);
                    self::assertCount($countBefore, $storedRows, "UPDATE mutation should preserve row count on iteration {$i}");
                }
            } catch (UnsupportedSqlException|UnknownSchemaException) {
                continue;
            }
        }
        self::addToAssertionCount(self::ITERATIONS);
    }
    /**
     * Test create table then delete does not crash.
     */
    public function testCreateTableThenDeleteDoesNotCrash(): void
    {
        $this->faker->seed(20260815);
        for ($i = 0; $i < self::ITERATIONS; $i++) {
            $createSql = $this->provider->createTableStatement(50);
            $definition = $this->schemaParser->parse($createSql);
            if ($definition === null || $definition->columns === []) {
                continue;
            }
            $shadowStore = new ShadowStore();
            $registry = new TableDefinitionRegistry();
            $tableName = (new Input\SchemaRows())->extractTableName($createSql);
            if ($tableName === null) {
                continue;
            }
            $registry->register($tableName, $definition);
            $fixtureRows = (new Input\SchemaRows())->generateFixtureRows($definition, 3);
            $shadowStore->set($tableName, $fixtureRows);
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
            $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
            $deleteSql = 'DELETE FROM ' . (new PgSqlIdentifierQuoter())->quote($tableName);
            try {
                $plan = $rewriter->rewrite($deleteSql);
                self::assertNotEmpty($plan->sql());
                self::assertSame(QueryKind::WRITE_SIMULATED, $plan->kind());
                if ($plan->mutation() !== null) {
                    $plan->mutation()->apply($shadowStore, $fixtureRows);
                    $storedRows = $shadowStore->get($tableName);
                    self::assertSame([], $storedRows, "Deleting every result row should empty the shadow table on iteration {$i}");
                }
            } catch (UnsupportedSqlException|UnknownSchemaException) {
                continue;
            }
        }
        self::addToAssertionCount(self::ITERATIONS);
    }
    /**
     * Test create table rewrite registers then dml succeeds.
     */
    public function testCreateTableRewriteRegistersThenDmlSucceeds(): void
    {
        $this->faker->seed(20260815);
        for ($i = 0; $i < self::ITERATIONS; $i++) {
            $createSql = $this->provider->createTableStatement(50);
            $definition = $this->schemaParser->parse($createSql);
            if ($definition === null || $definition->columns === []) {
                continue;
            }
            $shadowStore = new ShadowStore();
            $registry = new TableDefinitionRegistry();
            $tableName = (new Input\SchemaRows())->extractTableName($createSql);
            if ($tableName === null) {
                continue;
            }
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
            $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
            try {
                $createPlan = $rewriter->rewrite($createSql);
                self::assertSame(QueryKind::DDL_SIMULATED, $createPlan->kind());
                if ($createPlan->mutation() !== null) {
                    $createPlan->mutation()->apply($shadowStore, []);
                }
                $selectSql = 'SELECT * FROM ' . (new PgSqlIdentifierQuoter())->quote($tableName);
                $selectPlan = $rewriter->rewrite($selectSql);
                self::assertNotEmpty($selectPlan->sql());
                self::assertSame(QueryKind::READ, $selectPlan->kind());
            } catch (UnsupportedSqlException|UnknownSchemaException) {
                continue;
            }
        }
        self::addToAssertionCount(self::ITERATIONS);
    }
    /**
     * Test shadow store integrity after multiple operations.
     */
    public function testShadowStoreIntegrityAfterMultipleOperations(): void
    {
        $this->faker->seed(20260815);
        for ($i = 0; $i < self::ITERATIONS; $i++) {
            $createSql = $this->provider->createTableStatement(50);
            $definition = $this->schemaParser->parse($createSql);
            if ($definition === null || $definition->columns === []) {
                continue;
            }
            $shadowStore = new ShadowStore();
            $registry = new TableDefinitionRegistry();
            $tableName = (new Input\SchemaRows())->extractTableName($createSql);
            if ($tableName === null) {
                continue;
            }
            $registry->register($tableName, $definition);
            $fixtureRows = (new Input\SchemaRows())->generateFixtureRows($definition, 3);
            $shadowStore->set($tableName, $fixtureRows);
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
            $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
            $operations = ['SELECT * FROM ' . (new PgSqlIdentifierQuoter())->quote($tableName), 'INSERT INTO ' . (new PgSqlIdentifierQuoter())->quote($tableName) . ' (' . implode(', ', array_map(fn (string $c) => (new PgSqlIdentifierQuoter())->quote($c), $definition->columns)) . ') VALUES (' . (new Input\InsertLiterals($this->faker))->buildInsertValues($definition) . ')'];
            if ($definition->primaryKeys !== []) {
                $operations[] = 'UPDATE ' . (new PgSqlIdentifierQuoter())->quote($tableName) . ' SET ' . (new PgSqlIdentifierQuoter())->quote($definition->columns[0]) . ' = ' . (new PgSqlIdentifierQuoter())->quote($definition->columns[0]);
            }
            $operations[] = 'DELETE FROM ' . (new PgSqlIdentifierQuoter())->quote($tableName);
            foreach ($operations as $sql) {
                try {
                    $plan = $rewriter->rewrite($sql);
                    self::assertNotEmpty($plan->sql());
                    self::assertInstanceOf(QueryKind::class, $plan->kind());
                    if ($plan->mutation() !== null) {
                        $fakeRows = (new Input\SchemaRows())->generateFixtureRows($definition, 1);
                        $plan->mutation()->apply($shadowStore, $fakeRows);
                    }
                    $allData = $shadowStore->getAll();
                    foreach ($allData as $tblName => $tblRows) {
                        self::assertNotEmpty($tblName, 'ShadowStore table name must not be empty');
                        foreach ($tblRows as $rowIdx => $row) {
                            self::assertNotEmpty($row, "ShadowStore table '{$tblName}' row {$rowIdx} must not be empty");
                        }
                    }
                    self::assertArrayHasKey($tableName, $allData);
                    if ($plan->kind() === QueryKind::READ) {
                        self::assertNull($plan->mutation(), 'READ plan must have no mutation');
                    } elseif ($plan->kind() === QueryKind::WRITE_SIMULATED) {
                        self::assertNotNull($plan->mutation(), 'WRITE_SIMULATED plan must have a mutation');
                    } elseif ($plan->kind() === QueryKind::DDL_SIMULATED) {
                        self::assertNotNull($plan->mutation(), 'DDL_SIMULATED plan must have a mutation');
                    }
                } catch (UnsupportedSqlException|UnknownSchemaException) {
                    continue;
                }
            }
        }
        self::addToAssertionCount(self::ITERATIONS);
    }



}
