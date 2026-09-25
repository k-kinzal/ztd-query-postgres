<?php

declare(strict_types=1);

namespace Fuzz;

use Faker\Factory;
use Override;
use PHPUnit\Framework\Attributes\CoversNothing;
use PHPUnit\Framework\Attributes\Large;
use PHPUnit\Framework\TestCase;
use SqlFaker\PostgreSql\PostgreSqlProvider;
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
use ZtdQuery\Schema\TableDefinition;
use ZtdQuery\Schema\TableDefinitionRegistry;
use ZtdQuery\Shadow\ShadowStore;

/**
 * Fuzz tests for PgSqlRewriter::rewrite().
 *
 * Guards the following properties:
 * - INV-L2-01: rewrite() only throws UnsupportedSqlException or UnknownSchemaException (not arbitrary exceptions)
 * - INV-L2-02: WRITE_SIMULATED/DDL_SIMULATED plans must have non-null mutation
 * - INV-L2-03: READ plans must have null mutation
 * - INV-L2-04: Rewritten SQL must not be empty
 * - INV-L2-05: classify() and rewrite() must agree on QueryKind
 * - Kind correctness per SQL type: SELECT->READ, INSERT/UPDATE/DELETE->WRITE_SIMULATED, DDL->DDL_SIMULATED
 */
#[CoversNothing]
#[Large]
final class RewriteFuzzTest extends TestCase
{
    private const ITERATIONS = 100;
    private PgSqlRewriter $rewriter;
    private PgSqlQueryGuard $guard;
    private PostgreSqlProvider $provider;
    #[Override]
    protected function setUp(): void
    {
        $parser = new PgSqlParser();
        $this->guard = new PgSqlQueryGuard($parser);
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(['id', 'name', 'email'], ['id' => 'INTEGER', 'name' => 'TEXT', 'email' => 'TEXT'], ['id'], ['id'], []));
        $castRenderer = new PgSqlCastRenderer();
        $quoter = new PgSqlIdentifierQuoter();
        $selectTransformer = new SelectTransformer($castRenderer, $quoter);
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $this->rewriter = new PgSqlRewriter($this->guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $faker = Factory::create();
        $this->provider = new PostgreSqlProvider($faker, 'pg-17.2');
        $faker->seed(20260815);
    }
    /**
     * Test rewrite select returns read kind.
     */
    public function testRewriteSelectReturnsReadKind(): void
    {
        for ($i = 0; $i < self::ITERATIONS; $i++) {
            $sql = $this->provider->selectStatement(50);
            try {
                $plan = $this->rewriter->rewrite($sql);
                self::assertNotEmpty($plan->sql());
                self::assertSame(QueryKind::READ, $plan->kind(), "SELECT rewrite should produce READ kind on iteration {$i}");
                self::assertNull($plan->mutation(), "READ plan must have no mutation on iteration {$i}");
            } catch (UnsupportedSqlException|UnknownSchemaException) {
                continue;
            }
        }
        self::addToAssertionCount(self::ITERATIONS);
    }
    /**
     * Test rewrite insert returns write simulated kind.
     */
    public function testRewriteInsertReturnsWriteSimulatedKind(): void
    {
        for ($i = 0; $i < self::ITERATIONS; $i++) {
            $sql = $this->provider->insertStatement(50);
            try {
                $plan = $this->rewriter->rewrite($sql);
                self::assertNotEmpty($plan->sql());
                self::assertSame(QueryKind::WRITE_SIMULATED, $plan->kind(), "INSERT rewrite should produce WRITE_SIMULATED kind on iteration {$i}");
                self::assertNotNull($plan->mutation(), "WRITE_SIMULATED plan must have a mutation on iteration {$i}");
            } catch (UnsupportedSqlException|UnknownSchemaException) {
                continue;
            }
        }
        self::addToAssertionCount(self::ITERATIONS);
    }
    /**
     * Test rewrite update returns write simulated kind.
     */
    public function testRewriteUpdateReturnsWriteSimulatedKind(): void
    {
        for ($i = 0; $i < self::ITERATIONS; $i++) {
            $sql = $this->provider->updateStatement(50);
            try {
                $plan = $this->rewriter->rewrite($sql);
                self::assertNotEmpty($plan->sql());
                self::assertSame(QueryKind::WRITE_SIMULATED, $plan->kind(), "UPDATE rewrite should produce WRITE_SIMULATED kind on iteration {$i}");
                self::assertNotNull($plan->mutation(), "WRITE_SIMULATED plan must have a mutation on iteration {$i}");
            } catch (UnsupportedSqlException|UnknownSchemaException) {
                continue;
            }
        }
        self::addToAssertionCount(self::ITERATIONS);
    }
    /**
     * Test rewrite delete returns write simulated kind.
     */
    public function testRewriteDeleteReturnsWriteSimulatedKind(): void
    {
        for ($i = 0; $i < self::ITERATIONS; $i++) {
            $sql = $this->provider->deleteStatement(50);
            try {
                $plan = $this->rewriter->rewrite($sql);
                self::assertNotEmpty($plan->sql());
                self::assertSame(QueryKind::WRITE_SIMULATED, $plan->kind(), "DELETE rewrite should produce WRITE_SIMULATED kind on iteration {$i}");
                self::assertNotNull($plan->mutation(), "WRITE_SIMULATED plan must have a mutation on iteration {$i}");
            } catch (UnsupportedSqlException|UnknownSchemaException) {
                continue;
            }
        }
        self::addToAssertionCount(self::ITERATIONS);
    }
    /**
     * Test rewrite create table returns ddl simulated kind.
     */
    public function testRewriteCreateTableReturnsDdlSimulatedKind(): void
    {
        for ($i = 0; $i < self::ITERATIONS; $i++) {
            $sql = $this->provider->createTableStatement(50);
            try {
                $plan = $this->rewriter->rewrite($sql);
                self::assertNotEmpty($plan->sql());
                self::assertSame(QueryKind::DDL_SIMULATED, $plan->kind(), "CREATE TABLE rewrite should produce DDL_SIMULATED kind on iteration {$i}");
            } catch (UnsupportedSqlException|UnknownSchemaException) {
                continue;
            }
        }
        self::addToAssertionCount(self::ITERATIONS);
    }
    /**
     * Test rewrite drop table returns ddl simulated kind.
     */
    public function testRewriteDropTableReturnsDdlSimulatedKind(): void
    {
        for ($i = 0; $i < self::ITERATIONS; $i++) {
            $sql = $this->provider->dropTableStatement(50);
            try {
                $plan = $this->rewriter->rewrite($sql);
                self::assertNotEmpty($plan->sql());
                self::assertSame(QueryKind::DDL_SIMULATED, $plan->kind(), "DROP TABLE rewrite should produce DDL_SIMULATED kind on iteration {$i}");
            } catch (UnsupportedSqlException|UnknownSchemaException) {
                continue;
            }
        }
        self::addToAssertionCount(self::ITERATIONS);
    }
    /**
     * INV-L2-01: rewrite() must only throw UnsupportedSqlException or UnknownSchemaException.
     * INV-L2-02/03/04: Plan consistency (mutation presence, non-empty SQL).
     */
    public function testRewriteExceptionTypesAndPlanConsistency(): void
    {
        for ($i = 0; $i < self::ITERATIONS; $i++) {
            $sql = $this->provider->sql(maxDepth: 50);
            try {
                $plan = $this->rewriter->rewrite($sql);
                self::assertNotEmpty($plan->sql(), "Rewritten SQL is empty on iteration {$i}");
                if ($plan->kind() === QueryKind::WRITE_SIMULATED || $plan->kind() === QueryKind::DDL_SIMULATED) {
                    self::assertNotNull($plan->mutation(), "{$plan->kind()->value} plan must have mutation on iteration {$i}");
                }
                if ($plan->kind() === QueryKind::READ) {
                    self::assertNull($plan->mutation(), "READ plan must have no mutation on iteration {$i}");
                }
            } catch (UnsupportedSqlException|UnknownSchemaException) {
                continue;
            }
        }
        self::addToAssertionCount(self::ITERATIONS);
    }
    /**
     * INV-L2-05: classify() and rewrite() must agree on QueryKind.
     */
    public function testClassifyRewriteAgreement(): void
    {
        for ($i = 0; $i < self::ITERATIONS; $i++) {
            $sql = $this->provider->sql(maxDepth: 50);
            $classifyResult = $this->guard->classify($sql);
            if ($classifyResult === null) {
                continue;
            }
            try {
                $plan = $this->rewriter->rewrite($sql);
                self::assertSame($classifyResult, $plan->kind(), "classify() returned {$classifyResult->value} but rewrite() returned {$plan->kind()->value} on iteration {$i} with SQL: {$sql}");
            } catch (UnsupportedSqlException|UnknownSchemaException) {
                continue;
            }
        }
        self::addToAssertionCount(self::ITERATIONS);
    }
}
