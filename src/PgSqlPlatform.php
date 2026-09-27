<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres;

use ZtdQuery\Connection\ConnectionInterface;
use ZtdQuery\Platform;
use ZtdQuery\Platform\CopySupport;
use ZtdQuery\Platform\ParameterBindingCompiler;
use ZtdQuery\Platform\Postgres\Connection\Copy\PgSqlCopySupport;
use ZtdQuery\Platform\Postgres\Connection\Parameter\PgSqlPdoParameterBindingCompiler;
use ZtdQuery\Platform\Postgres\Connection\Result\PgSqlPdoResultColumnTypeResolver;
use ZtdQuery\Platform\Postgres\Rewrite\PgSqlQueryGuard;
use ZtdQuery\Platform\Postgres\Rewrite\PgSqlRewriter;
use ZtdQuery\Platform\Postgres\Rewrite\Transformer\DeleteTransformer;
use ZtdQuery\Platform\Postgres\Rewrite\Transformer\InsertTransformer;
use ZtdQuery\Platform\Postgres\Rewrite\Transformer\PgSqlTransformer;
use ZtdQuery\Platform\Postgres\Rewrite\Transformer\SelectTransformer;
use ZtdQuery\Platform\Postgres\Rewrite\Transformer\UpdateTransformer;
use ZtdQuery\Platform\Postgres\Schema\PgSqlSchemaParser;
use ZtdQuery\Platform\Postgres\Schema\PgSqlSchemaReflector;
use ZtdQuery\Platform\Postgres\Shadow\PgSqlMutationResolver;
use ZtdQuery\Platform\Postgres\Sql\PgSqlParser;
use ZtdQuery\Platform\ResultColumnTypeResolver;
use ZtdQuery\Rewrite\SqlRewriter;
use ZtdQuery\Schema\TableDefinitionRegistry;
use ZtdQuery\Schema\ViewDefinitionSet;
use ZtdQuery\Shadow\ShadowStore;

/**
 * Database semantics for PostgreSQL.
 *
 * Inject this platform into a QueryExecutor or a driver adapter. Each executor owns
 * its session state and receives an independent rewrite pipeline.
 * @visibility public
 * @example Inject database semantics without creating a session
 *     $platform = new \ZtdQuery\Platform\Postgres\PgSqlPlatform();
 *     $platform instanceof \ZtdQuery\Platform // => true
 */
final class PgSqlPlatform implements Platform
{
    /**
     * {@inheritDoc}
     */
    public function reflectSchema(ConnectionInterface $connection): TableDefinitionRegistry
    {
        $schemaParser = new PgSqlSchemaParser();
        $registry = new TableDefinitionRegistry();

        $reflector = new PgSqlSchemaReflector($connection);
        (new Schema\PgSqlSchemaInitializer())->populate($connection, $reflector, $schemaParser, $registry);

        return $registry;
    }

    /**
     * {@inheritDoc}
     */
    public function reflectViews(ConnectionInterface $connection): ViewDefinitionSet
    {
        $reflector = new PgSqlSchemaReflector($connection);
        $views = new ViewDefinitionSet();
        foreach ($reflector->reflectViews() as $viewName => $definition) {
            $views->register($viewName, $definition);
        }

        return $views;
    }

    /**
     * {@inheritDoc}
     */
    public function createRewriter(ShadowStore $store, TableDefinitionRegistry $registry, ViewDefinitionSet $views): SqlRewriter
    {
        $parser = new PgSqlParser();
        $schemaParser = new PgSqlSchemaParser();

        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $mutationResolver = new PgSqlMutationResolver($store, $registry, $schemaParser, $parser);
        return new PgSqlRewriter($guard, $store, $registry, $transformer, $mutationResolver, $parser, $views);
    }

    /**
     * {@inheritDoc}
     */
    public function copySupport(): CopySupport
    {
        return new PgSqlCopySupport();
    }

    /**
     * {@inheritDoc}
     */
    public function parameterBindingCompiler(): ParameterBindingCompiler
    {
        return new PgSqlPdoParameterBindingCompiler();
    }

    /**
     * {@inheritDoc}
     */
    public function resultColumnTypeResolver(): ResultColumnTypeResolver
    {
        return new PgSqlPdoResultColumnTypeResolver();
    }
}
