<?php

declare(strict_types=1);

namespace Bench;

use PhpBench\Attributes as Bench;
use ZtdQuery\Platform\Postgres\Rewrite\PgSqlQueryGuard;
use ZtdQuery\Platform\Postgres\Rewrite\PgSqlRewriter;
use ZtdQuery\Platform\Postgres\Rewrite\Transformer\DeleteTransformer;
use ZtdQuery\Platform\Postgres\Rewrite\Transformer\InsertTransformer;
use ZtdQuery\Platform\Postgres\Rewrite\Transformer\PgSqlTransformer;
use ZtdQuery\Platform\Postgres\Rewrite\Transformer\SelectTransformer;
use ZtdQuery\Platform\Postgres\Rewrite\Transformer\UpdateTransformer;
use ZtdQuery\Platform\Postgres\Schema\PgSqlSchemaParser;
use ZtdQuery\Platform\Postgres\Shadow\PgSqlMutationResolver;
use ZtdQuery\Platform\Postgres\Sql\PgSqlParser;
use ZtdQuery\Schema\TableDefinition;
use ZtdQuery\Schema\TableDefinitionRegistry;
use ZtdQuery\Shadow\ShadowStore;

/**
 * Measures PostgreSQL SELECT and INSERT rewriting against a registered schema.
 */
final class RewriterBench
{
    private PgSqlRewriter $rewriter;

    private string $selectSql = 'SELECT id, name, email FROM users WHERE id = 1';

    private string $insertSql = "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')";

    /**
     * Builds the PostgreSQL rewriter with an empty shadow store and a users schema.
     */
    public function setUp(): void
    {
        $store = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register(
            'users',
            new TableDefinition(
                ['id', 'name', 'email'],
                ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
                ['id'],
                ['id', 'name'],
                [],
            ),
        );

        $parser = new PgSqlParser();
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer(
            $parser,
            $selectTransformer,
            $insertTransformer,
            $updateTransformer,
            $deleteTransformer,
        );
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($store, $registry, $schemaParser, $parser);

        $this->rewriter = new PgSqlRewriter(
            new PgSqlQueryGuard($parser),
            $store,
            $registry,
            $transformer,
            $mutationResolver,
            $parser,
        );
    }

    /**
     * Rewrites a SELECT query against the registered users table.
     */
    #[Bench\BeforeMethods('setUp')]
    #[Bench\Revs(100)]
    public function benchRewriteSelect(): void
    {
        $this->rewriter->rewrite($this->selectSql);
    }

    /**
     * Rewrites an INSERT query into a result SELECT query.
     */
    #[Bench\BeforeMethods('setUp')]
    #[Bench\Revs(100)]
    public function benchRewriteInsert(): void
    {
        $this->rewriter->rewrite($this->insertSql);
    }
}
