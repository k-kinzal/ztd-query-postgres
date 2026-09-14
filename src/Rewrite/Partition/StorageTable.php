<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Rewrite\Partition;

use ZtdQuery\Schema\TableDefinitionRegistry;

/**
 * Storage table operations for PostgreSQL partition.
 *
 * @visibility root
 */
final class StorageTable
{
    private readonly TableDefinitionRegistry $registry;

    /**
     * Supplies the dependencies used by this StorageTable.
     */
    public function __construct(TableDefinitionRegistry $registry)
    {
        $this->registry = $registry;
    }

    /**
     * Storage table.
     */
    public function storageTable(string $tableName): string
    {
        $seen = [];
        while (!in_array($tableName, $seen, true)) {
            $seen[] = $tableName;
            $parent = $this->registry->get($tableName)?->partitionRelation?->parentTable;
            if ($parent === null) {
                return $tableName;
            }
            $tableName = $parent;
        }

        return $tableName;
    }
}
