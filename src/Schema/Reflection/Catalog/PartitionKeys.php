<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Schema\Reflection\Catalog;

use ZtdQuery\Connection\ConnectionInterface;
use ZtdQuery\Platform\Postgres\Sql\Partition\PgSqlPartitionParser;
use ZtdQuery\Schema\Partition\TablePartitionKey;

/**
 * Reflects the partition-key expression attached to each partitioned table.
 *
 * @visibility root
 */
final class PartitionKeys
{
    /**
     * Supplies access to the current PostgreSQL schema.
     */
    public function __construct(private readonly ConnectionInterface $connection)
    {
    }

    /**
     * Parses catalog partition-key declarations into typed metadata.
     * @return array<string, TablePartitionKey>
     */
    public function reflect(): array
    {
        $parser = new PgSqlPartitionParser();
        $keys = [];
        $keyStatement = $this->connection->query(
            'SELECT c.relname AS table_name, pg_get_partkeydef(c.oid) AS partition_key '
            . 'FROM pg_partitioned_table pt '
            . 'JOIN pg_class c ON c.oid = pt.partrelid '
            . 'JOIN pg_namespace n ON n.oid = c.relnamespace '
            . 'WHERE n.nspname = current_schema() ORDER BY c.relname',
        );
        if ($keyStatement !== false) {
            foreach ($keyStatement->fetchAll() as $row) {
                $tableName = $row['table_name'] ?? null;
                $partitionKey = $row['partition_key'] ?? null;
                if (!is_string($tableName) || $tableName === '' || !is_string($partitionKey)) {
                    continue;
                }
                $key = $parser->parseKey("PARTITION BY $partitionKey");
                if ($key !== null) {
                    $keys[$tableName] = $key;
                }
            }
        }

        return $keys;
    }
}
