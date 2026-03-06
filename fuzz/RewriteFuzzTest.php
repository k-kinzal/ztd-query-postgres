<?php

declare(strict_types=1);

namespace Fuzz;

use Faker\Factory;
use PHPUnit\Framework\TestCase;
use SqlFaker\PostgreSqlProvider;
use ZtdQuery\Exception\UnknownSchemaException;
use ZtdQuery\Exception\UnsupportedSqlException;
use ZtdQuery\Rewrite\QueryKind;
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
final class RewriteFuzzTest extends TestCase
{
    private const ITERATIONS = 100;

    private PgSqlRewriter $rewriter;

    private PgSqlQueryGuard $guard;

    private PostgreSqlProvider $provider;

    protected function setUp(): void
    {
        $parser = new PgSqlParser();
        $this->guard = new PgSqlQueryGuard($parser);
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();

        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'TEXT', 'email' => 'TEXT'],
            ['id'],
            ['id'],
            [],
        ));

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
        $this->provider = new PostgreSqlProvider($faker);
    }

    public function testRewriteSelectReturnsReadKind(): void
    {
        for ($i = 0; $i < self::ITERATIONS; $i++) {
            $sql = $this->provider->selectStatement(50);
            try {
                $plan = $this->rewriter->rewrite($sql);
                self::assertNotEmpty($plan->sql());
                self::assertSame(QueryKind::READ, $plan->kind(), "SELECT rewrite should produce READ kind on iteration $i");
                self::assertNull($plan->mutation(), "READ plan must have no mutation on iteration $i");
            } catch (UnsupportedSqlException|UnknownSchemaException) {
            } catch (\Throwable $e) {
                self::fail("rewrite() crashed on SELECT iteration $i with SQL: $sql\nError: " . $e->getMessage());
            }
        }
        self::addToAssertionCount(self::ITERATIONS);
    }

    public function testRewriteInsertReturnsWriteSimulatedKind(): void
    {
        for ($i = 0; $i < self::ITERATIONS; $i++) {
            $sql = $this->provider->insertStatement(50);
            try {
                $plan = $this->rewriter->rewrite($sql);
                self::assertNotEmpty($plan->sql());
                self::assertSame(QueryKind::WRITE_SIMULATED, $plan->kind(), "INSERT rewrite should produce WRITE_SIMULATED kind on iteration $i");
                self::assertNotNull($plan->mutation(), "WRITE_SIMULATED plan must have a mutation on iteration $i");
            } catch (UnsupportedSqlException|UnknownSchemaException) {
            } catch (\Throwable $e) {
                self::fail("rewrite() crashed on INSERT iteration $i with SQL: $sql\nError: " . $e->getMessage());
            }
        }
        self::addToAssertionCount(self::ITERATIONS);
    }

    public function testRewriteUpdateReturnsWriteSimulatedKind(): void
    {
        for ($i = 0; $i < self::ITERATIONS; $i++) {
            $sql = $this->provider->updateStatement(50);
            try {
                $plan = $this->rewriter->rewrite($sql);
                self::assertNotEmpty($plan->sql());
                self::assertSame(QueryKind::WRITE_SIMULATED, $plan->kind(), "UPDATE rewrite should produce WRITE_SIMULATED kind on iteration $i");
                self::assertNotNull($plan->mutation(), "WRITE_SIMULATED plan must have a mutation on iteration $i");
            } catch (UnsupportedSqlException|UnknownSchemaException) {
            } catch (\Throwable $e) {
                self::fail("rewrite() crashed on UPDATE iteration $i with SQL: $sql\nError: " . $e->getMessage());
            }
        }
        self::addToAssertionCount(self::ITERATIONS);
    }

    public function testRewriteDeleteReturnsWriteSimulatedKind(): void
    {
        for ($i = 0; $i < self::ITERATIONS; $i++) {
            $sql = $this->provider->deleteStatement(50);
            try {
                $plan = $this->rewriter->rewrite($sql);
                self::assertNotEmpty($plan->sql());
                self::assertSame(QueryKind::WRITE_SIMULATED, $plan->kind(), "DELETE rewrite should produce WRITE_SIMULATED kind on iteration $i");
                self::assertNotNull($plan->mutation(), "WRITE_SIMULATED plan must have a mutation on iteration $i");
            } catch (UnsupportedSqlException|UnknownSchemaException) {
            } catch (\Throwable $e) {
                self::fail("rewrite() crashed on DELETE iteration $i with SQL: $sql\nError: " . $e->getMessage());
            }
        }
        self::addToAssertionCount(self::ITERATIONS);
    }

    public function testRewriteCreateTableReturnsDdlSimulatedKind(): void
    {
        for ($i = 0; $i < self::ITERATIONS; $i++) {
            $sql = $this->provider->createTableStatement(50);
            try {
                $plan = $this->rewriter->rewrite($sql);
                self::assertNotEmpty($plan->sql());
                self::assertSame(QueryKind::DDL_SIMULATED, $plan->kind(), "CREATE TABLE rewrite should produce DDL_SIMULATED kind on iteration $i");
            } catch (UnsupportedSqlException|UnknownSchemaException) {
            } catch (\Throwable $e) {
                self::fail("rewrite() crashed on CREATE TABLE iteration $i with SQL: $sql\nError: " . $e->getMessage());
            }
        }
        self::addToAssertionCount(self::ITERATIONS);
    }

    public function testRewriteDropTableReturnsDdlSimulatedKind(): void
    {
        for ($i = 0; $i < self::ITERATIONS; $i++) {
            $sql = $this->provider->dropTableStatement(50);
            try {
                $plan = $this->rewriter->rewrite($sql);
                self::assertNotEmpty($plan->sql());
                self::assertSame(QueryKind::DDL_SIMULATED, $plan->kind(), "DROP TABLE rewrite should produce DDL_SIMULATED kind on iteration $i");
            } catch (UnsupportedSqlException|UnknownSchemaException) {
            } catch (\Throwable $e) {
                self::fail("rewrite() crashed on DROP TABLE iteration $i with SQL: $sql\nError: " . $e->getMessage());
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
                self::assertNotEmpty($plan->sql(), "Rewritten SQL is empty on iteration $i");
                if ($plan->kind() === QueryKind::WRITE_SIMULATED || $plan->kind() === QueryKind::DDL_SIMULATED) {
                    self::assertNotNull($plan->mutation(), "{$plan->kind()->value} plan must have mutation on iteration $i");
                }
                if ($plan->kind() === QueryKind::READ) {
                    self::assertNull($plan->mutation(), "READ plan must have no mutation on iteration $i");
                }
            } catch (UnsupportedSqlException|UnknownSchemaException) {
            } catch (\Throwable $e) {
                self::fail("rewrite() threw unexpected " . get_class($e) . " on iteration $i with SQL: $sql\nError: " . $e->getMessage());
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
            try {
                $classifyResult = $this->guard->classify($sql);
            } catch (\Throwable) {
                continue;
            }

            if ($classifyResult === null) {
                continue;
            }

            try {
                $plan = $this->rewriter->rewrite($sql);
                self::assertSame(
                    $classifyResult,
                    $plan->kind(),
                    "classify() returned {$classifyResult->value} but rewrite() returned {$plan->kind()->value} on iteration $i with SQL: $sql"
                );
            } catch (UnsupportedSqlException|UnknownSchemaException) {
            } catch (\Throwable $e) {
                self::fail("rewrite() crashed on iteration $i with SQL: $sql\nError: " . $e->getMessage());
            }
        }
        self::addToAssertionCount(self::ITERATIONS);
    }
}
