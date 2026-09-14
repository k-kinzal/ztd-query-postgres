<?php

declare(strict_types=1);

namespace Fuzz;

use Error;
use Fuzz\Input\TruncateTargets;
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
use ZtdQuery\Schema\Key\PartialUniqueIndex;
use ZtdQuery\Schema\TableDefinitionRegistry;
use ZtdQuery\Shadow\Mutation\Table\MultiTruncateMutation;
use ZtdQuery\Shadow\ShadowStore;

/**
 * Checks one rewrite against its routing and mutation contracts with isolated state.
 */
final class RewriteCheck
{
    /**
     * Accepts only documented unsupported SQL and missing-schema rejections.
     *
     * SQLFaker guarantees grammar generation, not names or result rows for this catalog.
     * The full target supplies an empty result set to exercise zero-row mutation handling;
     * it does not assert equivalence with PostgreSQL execution.
     *
     * @throws Error When an accepted rewrite violates its public plan contract
     */
    public function verify(string $sql, bool $applyMutation): void
    {
        $store = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $schemaParser = new PgSqlSchemaParser();
        $schemas = ['users' => 'CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(255) NOT NULL, email VARCHAR(255), status VARCHAR(50))', 'orders' => 'CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, amount NUMERIC(10,2), created_at TIMESTAMP)', 'order_items' => 'CREATE TABLE order_items (order_id INTEGER NOT NULL, product_id INTEGER NOT NULL, quantity INTEGER NOT NULL DEFAULT 1, PRIMARY KEY (order_id, product_id))', 'products' => 'CREATE TABLE products (id INTEGER PRIMARY KEY, name VARCHAR(255) NOT NULL, price NUMERIC(10,2), category VARCHAR(100))', 'logs' => 'CREATE TABLE logs (id INTEGER NOT NULL, log_date DATE NOT NULL, level TEXT NOT NULL, PRIMARY KEY (id, log_date)) PARTITION BY RANGE (log_date)', 'contacts' => 'CREATE TABLE contacts (id INTEGER PRIMARY KEY, age "ztd_fuzz"."positive_int", satisfaction "ztd_fuzz"."percentage")'];
        foreach ($schemas as $tableName => $createSql) {
            $definition = $schemaParser->parse($createSql);
            if ($definition !== null) {
                if ($tableName === 'users') {
                    $definition = $definition->withPartialUniqueIndex(new PartialUniqueIndex('users_active_email', ['email'], "status = 'active'"));
                }
                $registry->register($tableName, $definition);
            }
        }
        $store->set('users', [['id' => '1', 'name' => 'Alice', 'email' => 'alice@example.com', 'status' => 'active'], ['id' => '2', 'name' => 'Bob', 'email' => 'bob@example.com', 'status' => 'pending'], ['id' => '3', 'name' => 'Charlie', 'email' => null, 'status' => 'active']]);
        $store->set('orders', [['id' => '1', 'user_id' => '1', 'amount' => '100.00', 'created_at' => '2024-01-01 00:00:00'], ['id' => '2', 'user_id' => '2', 'amount' => '250.50', 'created_at' => '2024-01-02 12:30:00']]);
        $store->set('order_items', [['order_id' => '1', 'product_id' => '1', 'quantity' => '2'], ['order_id' => '1', 'product_id' => '2', 'quantity' => '1'], ['order_id' => '2', 'product_id' => '1', 'quantity' => '3']]);
        $store->set('products', [['id' => '1', 'name' => 'Widget', 'price' => '19.99', 'category' => 'tools'], ['id' => '2', 'name' => 'Gadget', 'price' => '49.99', 'category' => 'electronics']]);
        $store->set('logs', [['id' => '1', 'log_date' => '2024-05-01', 'level' => 'INFO'], ['id' => '2', 'log_date' => '2025-05-01', 'level' => 'WARN']]);
        $store->set('contacts', [['id' => '1', 'age' => '30', 'satisfaction' => '85.50']]);
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
        $mutationResolver = new PgSqlMutationResolver($store, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $store, $registry, $transformer, $mutationResolver, $parser);
        $kind = (new PgSqlQueryGuard(new PgSqlParser()))->classify($sql);
        try {
            $plan = $rewriter->rewrite($sql);
        } catch (UnsupportedSqlException|UnknownSchemaException) {
            return;
        }
        if ($plan->kind() !== $kind || $plan->sql() === '') {
            throw new Error('The accepted rewrite must preserve classification and return nonempty SQL.');
        }
        $mutation = $plan->mutation();
        $writes = $kind === QueryKind::WRITE_SIMULATED || $kind === QueryKind::DDL_SIMULATED;
        if ($writes !== ($mutation !== null)) {
            throw new Error('Only a simulated write or DDL plan may own a mutation.');
        }
        $targets = (new TruncateTargets())->targetCount($sql);
        if ($targets !== null && $targets > 1 && (!$mutation instanceof MultiTruncateMutation || count($mutation->tableNames()) !== $targets)) {
            throw new Error('TRUNCATE must retain every explicit target in its mutation.');
        }
        if ($applyMutation && $mutation !== null) {
            $mutation->apply($store, []);
            $rewriter->commitRewriteState();
            if (array_key_exists('', $store->getAll())) {
                throw new Error('Applying a mutation must not introduce an empty table name.');
            }
        }
    }
}
