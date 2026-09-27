<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Schema;

use ZtdQuery\Connection\ConnectionInterface;
use ZtdQuery\Platform\Postgres\Schema\Partition\PgSqlPartitionReflector;
use ZtdQuery\Schema\TableDefinitionRegistry;

/**
 * Registers reflected PostgreSQL metadata for the PostgreSQL platform.
 *
 * @visibility root
 */
final class PgSqlSchemaInitializer
{
    /**
     * Loads table definitions, partial indexes and partition relationships in reflection order.
     */
    public function populate(ConnectionInterface $connection, PgSqlSchemaReflector $reflector, PgSqlSchemaParser $schemaParser, TableDefinitionRegistry $registry): void
    {
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
    }
}
