<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres;

use ZtdQuery\Config\ZtdConfig;
use ZtdQuery\Connection\ConnectionInterface;
use ZtdQuery\Platform\Postgres\Transformer\DeleteTransformer;
use ZtdQuery\Platform\Postgres\Transformer\InsertTransformer;
use ZtdQuery\Platform\Postgres\Transformer\SelectTransformer;
use ZtdQuery\Platform\Postgres\Transformer\UpdateTransformer;
use ZtdQuery\ResultSelectRunner;
use ZtdQuery\Schema\TableDefinitionRegistry;
use ZtdQuery\Schema\ViewDefinitionSet;
use ZtdQuery\Session;
use ZtdQuery\Platform\SessionFactory;
use ZtdQuery\Shadow\ShadowStore;
use ZtdQuery\Shadow\ShadowTransactionManager;

/**
 * Factory for creating Session instances pre-configured for PostgreSQL.
 */
final class PgSqlSessionFactory implements SessionFactory
{
    /**
     * {@inheritDoc}
     */
    public function create(ConnectionInterface $connection, ZtdConfig $config): Session
    {
        $shadowStore = new ShadowStore();
        $parser = new PgSqlParser();
        $schemaParser = new PgSqlSchemaParser();
        $registry = new TableDefinitionRegistry();

        $reflector = new PgSqlSchemaReflector($connection);
        foreach ($reflector->reflectAll() as $tableName => $createSql) {
            $definition = $schemaParser->parse($createSql);
            if ($definition !== null) {
                $registry->register($tableName, $definition);
            }
        }
        foreach ($reflector->partialUniqueIndexes() as $tableName => $indexes) {
            $definition = $registry->get($tableName);
            if ($definition === null) {
                continue;
            }
            foreach ($indexes as $index) {
                $definition = $definition->withPartialUniqueIndex($index);
            }
            $registry->register($tableName, $definition);
        }
        $partitionMetadata = (new PgSqlPartitionReflector($connection))->reflect();
        foreach ($partitionMetadata['keys'] as $tableName => $partitionKey) {
            $definition = $registry->get($tableName);
            if ($definition !== null) {
                $registry->register($tableName, $definition->withPartitionKey($partitionKey));
            }
        }
        foreach ($partitionMetadata['relations'] as $tableName => $partitionRelation) {
            $definition = $registry->get($tableName);
            if ($definition !== null) {
                $registry->register($tableName, $definition->withPartitionRelation($partitionRelation));
            }
        }
        $views = new ViewDefinitionSet();
        foreach ($reflector->reflectViews() as $viewName => $definition) {
            $views->register($viewName, $definition);
        }

        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser, $views);

        return new Session(
            $rewriter,
            $shadowStore,
            new ResultSelectRunner(),
            $config,
            $connection,
            transactions: new ShadowTransactionManager($shadowStore, $registry),
            registry: $registry,
            copySupport: new PgSqlCopySupport(),
            parameterBindingCompiler: new PgSqlPdoParameterBindingCompiler(),
            resultColumnTypeResolver: new PgSqlPdoResultColumnTypeResolver(),
        );
    }
}
