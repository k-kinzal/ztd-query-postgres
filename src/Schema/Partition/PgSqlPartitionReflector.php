<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Schema\Partition;

use ZtdQuery\Connection\ConnectionInterface;
use ZtdQuery\Schema\Partition\TablePartitionKey;
use ZtdQuery\Schema\Partition\TablePartitionRelation;

/**
 * Partition reflector for PostgreSQL queries.
 */
final class PgSqlPartitionReflector
{
    /**
     * Initializes the collaborators and state used by this partition reflector.
     */
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
        $keys = (new \ZtdQuery\Platform\Postgres\Schema\Reflection\Catalog\PartitionKeys($this->connection))->reflect();

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
