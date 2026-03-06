<?php

declare(strict_types=1);

namespace Bench;

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

final class RewriterBench
{
    private PgSqlRewriter $rewriter;

    private string $selectSql = 'SELECT id, name, email FROM users WHERE id = 1';

    private string $insertSql = "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')";

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
     * @BeforeMethods({"setUp"})
     * @Revs(100)
     * @Iterations(5)
     */
    public function benchRewriteSelect(): void
    {
        $this->rewriter->rewrite($this->selectSql);
    }

    /**
     * @BeforeMethods({"setUp"})
     * @Revs(100)
     * @Iterations(5)
     */
    public function benchRewriteInsert(): void
    {
        $this->rewriter->rewrite($this->insertSql);
    }
}
