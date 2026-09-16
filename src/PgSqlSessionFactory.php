<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres;

use ZtdQuery\Config\ZtdConfig;
use ZtdQuery\Connection\ConnectionInterface;
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
use ZtdQuery\Platform\SessionFactory;
use ZtdQuery\ResultSelectRunner;
use ZtdQuery\Schema\TableDefinitionRegistry;
use ZtdQuery\Schema\ViewDefinitionSet;
use ZtdQuery\Session;
use ZtdQuery\Shadow\ShadowStore;
use ZtdQuery\Shadow\ShadowTransactions;

/**
 * Factory for creating Session instances pre-configured for PostgreSQL.
 *
 * @visibility public
 * @example Create a session with a successful empty catalog connection
 *     $statement = new class implements \ZtdQuery\Connection\StatementInterface { public function execute(?array $params = null): bool { return true; } public function fetchAll(): array { return []; } public function resultColumns(\ZtdQuery\Platform\ResultColumnTypeResolver $typeResolver): array { return []; } public function rowCount(): int { return 0; } };
 *     $connection = new class($statement) implements \ZtdQuery\Connection\ConnectionInterface { public function __construct(private \ZtdQuery\Connection\StatementInterface $statement) {} public function query(string $sql): \ZtdQuery\Connection\StatementInterface|false { return $this->statement; } };
 *     $session = (new \ZtdQuery\Platform\Postgres\PgSqlSessionFactory())->create($connection, new \ZtdQuery\Config\ZtdConfig());
 *     $session->isEnabled() // => true
 *     $session->splitStatements('SELECT 1; SELECT 2') // => ['SELECT 1', 'SELECT 2']
 */
final class PgSqlSessionFactory implements SessionFactory
{
    /**
     * {@inheritDoc}
     * @visibility public
     * @example Configure a session for a custom connection adapter
     *     $statement = new class implements \ZtdQuery\Connection\StatementInterface { public function execute(?array $params = null): bool { return true; } public function fetchAll(): array { return []; } public function resultColumns(\ZtdQuery\Platform\ResultColumnTypeResolver $typeResolver): array { return []; } public function rowCount(): int { return 0; } };
     *     $connection = new class($statement) implements \ZtdQuery\Connection\ConnectionInterface { public function __construct(private \ZtdQuery\Connection\StatementInterface $statement) {} public function query(string $sql): \ZtdQuery\Connection\StatementInterface|false { return $this->statement; } };
     *     $session = (new \ZtdQuery\Platform\Postgres\PgSqlSessionFactory())->create($connection, new \ZtdQuery\Config\ZtdConfig());
     *     $session->isEnabled() // => true
     *     $session->splitStatements('SELECT 1; SELECT 2') // => ['SELECT 1', 'SELECT 2']
     */
    public function create(ConnectionInterface $connection, ZtdConfig $config): Session
    {
        $shadowStore = new ShadowStore();
        $parser = new PgSqlParser();
        $schemaParser = new PgSqlSchemaParser();
        $registry = new TableDefinitionRegistry();

        $reflector = new PgSqlSchemaReflector($connection);
        (new \ZtdQuery\Platform\Postgres\Session\SchemaInitializer())->populate($connection, $reflector, $schemaParser, $registry);
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
            transactions: new ShadowTransactions($shadowStore, $registry),
            registry: $registry,
            copySupport: new PgSqlCopySupport(),
            parameterBindingCompiler: new PgSqlPdoParameterBindingCompiler(),
            resultColumnTypeResolver: new PgSqlPdoResultColumnTypeResolver(),
        );
    }
}
