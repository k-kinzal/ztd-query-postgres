<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Schema\Reflection\Key;

/**
 * Narrows catalog fields before assembling composite foreign keys.
 *
 * @visibility root
 */
final class ForeignKeyRow
{
    /**
     * Validates the string fields used to reconstruct a foreign-key constraint.
     * @template T
     * @param array<string, T> $foreignKeyRow
     * @return array{name: string, column: string, table: string, referencedColumn: string, onUpdate: string, onDelete: string}|null
     */
    public function parse(array $foreignKeyRow): ?array
    {
        $constraintName = $foreignKeyRow['constraint_name'] ?? null;
        $columnName = $foreignKeyRow['column_name'] ?? null;
        $foreignTable = $foreignKeyRow['foreign_table_name'] ?? null;
        $foreignColumn = $foreignKeyRow['foreign_column_name'] ?? null;
        $onUpdate = $foreignKeyRow['update_rule'] ?? null;
        $onDelete = $foreignKeyRow['delete_rule'] ?? null;
        if (!is_string($constraintName)
            || !is_string($columnName)
            || !is_string($foreignTable)
            || !is_string($foreignColumn)
            || !is_string($onUpdate)
            || !is_string($onDelete)
        ) {
            return null;
        }
        return ['name' => $constraintName, 'column' => $columnName, 'table' => $foreignTable, 'referencedColumn' => $foreignColumn, 'onUpdate' => $onUpdate, 'onDelete' => $onDelete];
    }
}
