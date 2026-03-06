<?php

declare(strict_types=1);

namespace Fuzz\Robustness\Target;

use Faker\Generator;
use Fuzz\Robustness\Invariant\ClassifyDeterministicChecker;
use Fuzz\Robustness\Invariant\ClassifyNeverThrowsChecker;
use Fuzz\Robustness\Invariant\ClassifyRewriteAgreementChecker;
use Fuzz\Robustness\Invariant\InvariantChecker;
use Fuzz\Robustness\Invariant\RewriteExceptionTypeChecker;
use Fuzz\Robustness\Invariant\RewritePlanConsistencyChecker;
use Fuzz\Robustness\Invariant\ShadowStoreConsistencyChecker;
use SqlFaker\PostgreSqlProvider;
use Throwable;
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
use ZtdQuery\Schema\TableDefinitionRegistry;
use ZtdQuery\Shadow\ShadowStore;

final class RobustnessTarget
{
    private Generator $faker;
    private PostgreSqlProvider $provider;
    private PgSqlRewriter $rewriter;
    private ShadowStore $shadowStore;
    private ShadowStoreConsistencyChecker $storeChecker;
    /** @var array<int, InvariantChecker> */
    private array $checkers;
    /** @var array<string, array<int, array<string, mixed>>> */
    private array $fixtureData;

    public function __construct(Generator $faker, PostgreSqlProvider $provider)
    {
        $this->faker = $faker;
        $this->provider = $provider;

        $parser = new PgSqlParser();
        $schemaParser = new PgSqlSchemaParser();
        $guard = new PgSqlQueryGuard($parser);
        $this->shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();

        $this->registerFixtureSchemas($registry, $schemaParser);
        $this->fixtureData = $this->buildFixtureData();
        $this->resetShadowStore();

        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $mutationResolver = new PgSqlMutationResolver($this->shadowStore, $registry, $schemaParser, $parser);
        $this->rewriter = new PgSqlRewriter($guard, $this->shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $this->storeChecker = new ShadowStoreConsistencyChecker($this->shadowStore);

        $this->checkers = [
            new ClassifyNeverThrowsChecker($guard),
            new ClassifyDeterministicChecker($guard),
            new RewriteExceptionTypeChecker($this->rewriter),
            new RewritePlanConsistencyChecker($this->rewriter),
            new ClassifyRewriteAgreementChecker($guard, $this->rewriter),
        ];
    }

    public function __invoke(string $input): void
    {
        $seed = crc32(str_pad($input, 4, "\0"));
        $this->faker->seed($seed);

        $sql = $this->selectGenerator($input)();

        foreach ($this->checkers as $checker) {
            $violation = $checker->check($sql);
            if ($violation !== null) {
                throw new \Error("Invariant violation: seed=$seed\n$violation");
            }
        }

        try {
            $plan = $this->rewriter->rewrite($sql);

            if ($plan->kind() === QueryKind::WRITE_SIMULATED || $plan->kind() === QueryKind::DDL_SIMULATED) {
                $mutation = $plan->mutation();
                if ($mutation !== null) {
                    try {
                        $mutation->apply($this->shadowStore, []);
                    } catch (Throwable) {
                    }

                    $violation = $this->storeChecker->check($sql);
                    if ($violation !== null) {
                        throw new \Error("Invariant violation: seed=$seed\n$violation");
                    }
                }
            }
        } catch (Throwable) {
        }

        $this->resetShadowStore();
    }

    private function resetShadowStore(): void
    {
        $this->shadowStore->clear();
        foreach ($this->fixtureData as $table => $rows) {
            $this->shadowStore->set($table, $rows);
        }
    }

    private function registerFixtureSchemas(TableDefinitionRegistry $registry, PgSqlSchemaParser $schemaParser): void
    {
        $schemas = [
            'users' => 'CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(255) NOT NULL, email VARCHAR(255), status VARCHAR(50))',
            'orders' => 'CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, amount NUMERIC(10,2), created_at TIMESTAMP)',
            'order_items' => 'CREATE TABLE order_items (order_id INTEGER NOT NULL, product_id INTEGER NOT NULL, quantity INTEGER NOT NULL DEFAULT 1, PRIMARY KEY (order_id, product_id))',
            'products' => 'CREATE TABLE products (id INTEGER PRIMARY KEY, name VARCHAR(255) NOT NULL, price NUMERIC(10,2), category VARCHAR(100))',
        ];

        foreach ($schemas as $tableName => $createSql) {
            $definition = $schemaParser->parse($createSql);
            if ($definition !== null) {
                $registry->register($tableName, $definition);
            }
        }
    }

    /**
     * @return array<string, array<int, array<string, mixed>>>
     */
    private function buildFixtureData(): array
    {
        return [
            'users' => [
                ['id' => '1', 'name' => 'Alice', 'email' => 'alice@example.com', 'status' => 'active'],
                ['id' => '2', 'name' => 'Bob', 'email' => 'bob@example.com', 'status' => 'pending'],
                ['id' => '3', 'name' => 'Charlie', 'email' => null, 'status' => 'active'],
            ],
            'orders' => [
                ['id' => '1', 'user_id' => '1', 'amount' => '100.00', 'created_at' => '2024-01-01 00:00:00'],
                ['id' => '2', 'user_id' => '2', 'amount' => '250.50', 'created_at' => '2024-01-02 12:30:00'],
            ],
            'order_items' => [
                ['order_id' => '1', 'product_id' => '1', 'quantity' => '2'],
                ['order_id' => '1', 'product_id' => '2', 'quantity' => '1'],
                ['order_id' => '2', 'product_id' => '1', 'quantity' => '3'],
            ],
            'products' => [
                ['id' => '1', 'name' => 'Widget', 'price' => '19.99', 'category' => 'tools'],
                ['id' => '2', 'name' => 'Gadget', 'price' => '49.99', 'category' => 'electronics'],
            ],
        ];
    }

    /**
     * @return callable(): string
     */
    private function selectGenerator(string $input): callable
    {
        $generators = [
            fn (): string => $this->provider->sql(maxDepth: 8),
            fn (): string => $this->provider->selectStatement(maxDepth: 8),
            fn (): string => $this->provider->insertStatement(maxDepth: 8),
            fn (): string => $this->provider->updateStatement(maxDepth: 8),
            fn (): string => $this->provider->deleteStatement(maxDepth: 8),
            fn (): string => $this->provider->createTableStatement(maxDepth: 5),
            fn (): string => $this->provider->alterTableStatement(maxDepth: 5),
            fn (): string => $this->provider->dropTableStatement(maxDepth: 3),
        ];

        $index = ord($input[0] ?? "\0") % count($generators);
        return $generators[$index];
    }
}
