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
use Fuzz\Robustness\Invariant\TruncateTargetConsistencyChecker;
use SqlFaker\PostgreSqlProvider;
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
use ZtdQuery\Schema\TableDefinitionRegistry;
use ZtdQuery\Schema\PartialUniqueIndex;
use ZtdQuery\Shadow\ShadowStore;

final class RewriteTarget
{
    private Generator $faker;
    private PostgreSqlProvider $provider;
    /** @var array<int, InvariantChecker> */
    private array $checkers;

    public function __construct(Generator $faker, PostgreSqlProvider $provider)
    {
        $this->faker = $faker;
        $this->provider = $provider;

        $parser = new PgSqlParser();
        $schemaParser = new PgSqlSchemaParser();
        $guard = new PgSqlQueryGuard($parser);
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();

        $this->registerFixtureSchemas($registry, $schemaParser);
        $this->populateFixtureData($shadowStore);

        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);

        $this->checkers = [
            new ClassifyNeverThrowsChecker($guard),
            new ClassifyDeterministicChecker($guard),
            new RewriteExceptionTypeChecker($rewriter),
            new RewritePlanConsistencyChecker($rewriter),
            new ClassifyRewriteAgreementChecker($guard, $rewriter),
            new TruncateTargetConsistencyChecker($rewriter),
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
    }

    private function registerFixtureSchemas(TableDefinitionRegistry $registry, PgSqlSchemaParser $schemaParser): void
    {
        $schemas = [
            'users' => 'CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(255) NOT NULL, email VARCHAR(255), status VARCHAR(50))',
            'orders' => 'CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, amount NUMERIC(10,2), created_at TIMESTAMP)',
            'order_items' => 'CREATE TABLE order_items (order_id INTEGER NOT NULL, product_id INTEGER NOT NULL, quantity INTEGER NOT NULL DEFAULT 1, PRIMARY KEY (order_id, product_id))',
            'products' => 'CREATE TABLE products (id INTEGER PRIMARY KEY, name VARCHAR(255) NOT NULL, price NUMERIC(10,2), category VARCHAR(100))',
            'logs' => 'CREATE TABLE logs (id INTEGER NOT NULL, log_date DATE NOT NULL, level TEXT NOT NULL, PRIMARY KEY (id, log_date)) PARTITION BY RANGE (log_date)',
            'contacts' => 'CREATE TABLE contacts (id INTEGER PRIMARY KEY, age "ztd_fuzz"."positive_int", satisfaction "ztd_fuzz"."percentage")',
        ];

        foreach ($schemas as $tableName => $createSql) {
            $definition = $schemaParser->parse($createSql);
            if ($definition !== null) {
                if ($tableName === 'users') {
                    $definition = $definition->withPartialUniqueIndex(
                        new PartialUniqueIndex('users_active_email', ['email'], "status = 'active'"),
                    );
                }
                $registry->register($tableName, $definition);
            }
        }
    }

    private function populateFixtureData(ShadowStore $store): void
    {
        $store->set('users', [
            ['id' => '1', 'name' => 'Alice', 'email' => 'alice@example.com', 'status' => 'active'],
            ['id' => '2', 'name' => 'Bob', 'email' => 'bob@example.com', 'status' => 'pending'],
            ['id' => '3', 'name' => 'Charlie', 'email' => null, 'status' => 'active'],
        ]);
        $store->set('orders', [
            ['id' => '1', 'user_id' => '1', 'amount' => '100.00', 'created_at' => '2024-01-01 00:00:00'],
            ['id' => '2', 'user_id' => '2', 'amount' => '250.50', 'created_at' => '2024-01-02 12:30:00'],
        ]);
        $store->set('order_items', [
            ['order_id' => '1', 'product_id' => '1', 'quantity' => '2'],
            ['order_id' => '1', 'product_id' => '2', 'quantity' => '1'],
            ['order_id' => '2', 'product_id' => '1', 'quantity' => '3'],
        ]);
        $store->set('products', [
            ['id' => '1', 'name' => 'Widget', 'price' => '19.99', 'category' => 'tools'],
            ['id' => '2', 'name' => 'Gadget', 'price' => '49.99', 'category' => 'electronics'],
        ]);
        $store->set('logs', [
            ['id' => '1', 'log_date' => '2024-05-01', 'level' => 'INFO'],
            ['id' => '2', 'log_date' => '2025-05-01', 'level' => 'WARN'],
        ]);
        $store->set('contacts', [
            ['id' => '1', 'age' => '30', 'satisfaction' => '85.50'],
        ]);
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
            fn (): string => $this->provider->truncateStatement(maxDepth: 8),
            fn (): string => $this->provider->insertFunctionUpsertStatement(),
            fn (): string => $this->provider->temporaryTableStatement(),
            fn (): string => $this->provider->viewStatement(),
            fn (): string => $this->provider->generatedColumnStatement(),
            fn (): string => $this->provider->foreignKeyCascadeStatement(),
            fn (): string => $this->provider->partitionOfStatement(),
            fn (): string => $this->provider->tableSampleStatement(),
            fn (): string => $this->provider->doStatement(),
            fn (): string => $this->provider->mergeStatement(),
            fn (): string => $this->provider->copyStatement(maxDepth: 8),
            fn (): string => $this->provider->partialIndexUpsertStatement(),
            fn (): string => $this->provider->createDomainStatement(maxDepth: 8),
            fn (): string => $this->provider->domainDmlStatement(),
            fn (): string => $this->provider->fullTextSearchStatement(),
        ];

        $index = ord($input[0] ?? "\0") % count($generators);
        return $generators[$index];
    }
}
