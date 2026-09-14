<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Sql\Conflict;

/**
 * Column set operations for PostgreSQL conflict.
 *
 * @visibility root
 */
final class ColumnSet
{
    /**
     * @param array<int, string> $columns
     * @return list<string>
     */
    public static function normalizedColumns(array $columns): array
    {
        $normalized = [];
        foreach ($columns as $column) {
            $normalized[] = strtolower($column);
        }
        sort($normalized);

        return $normalized;
    }
}
