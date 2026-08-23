<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres;

use ZtdQuery\Connection\ConnectionInterface;
use ZtdQuery\Schema\TablePartitionKey;
use ZtdQuery\Schema\TablePartitionRelation;

final class PgSqlPartitionReflector
{
    public function __construct(private readonly ConnectionInterface $connection)
    {
    }

    /**
     * @return array{
     *     keys: array<string, TablePartitionKey>,
     *     relations: array<string, TablePartitionRelation>
     * }
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

        $relations = [];
        $relationStatement = $this->connection->query(
            'SELECT child.relname AS child_table, parent.relname AS parent_table, '
            . 'pg_get_partition_constraintdef(child.oid) AS predicate '
            . 'FROM pg_inherits i '
            . 'JOIN pg_class child ON child.oid = i.inhrelid '
            . 'JOIN pg_namespace child_ns ON child_ns.oid = child.relnamespace '
            . 'JOIN pg_class parent ON parent.oid = i.inhparent '
            . 'JOIN pg_partitioned_table pt ON pt.partrelid = parent.oid '
            . 'WHERE child_ns.nspname = current_schema() ORDER BY child.relname',
        );
        if ($relationStatement !== false) {
            foreach ($relationStatement->fetchAll() as $row) {
                $childTable = $row['child_table'] ?? null;
                $parentTable = $row['parent_table'] ?? null;
                $predicate = $row['predicate'] ?? null;
                if (!is_string($childTable)
                    || $childTable === ''
                    || !is_string($parentTable)
                    || $parentTable === ''
                    || !is_string($predicate)
                    || trim($predicate) === ''
                ) {
                    continue;
                }
                $relations[$childTable] = new TablePartitionRelation($parentTable, $predicate);
            }
        }

        return ['keys' => $keys, 'relations' => $relations];
    }
}
