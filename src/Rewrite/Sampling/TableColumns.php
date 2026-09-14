<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Rewrite\Sampling;

/**
 * Table columns operations for PostgreSQL sampling.
 *
 * @visibility root
 */
final class TableColumns
{
    /**
     * @template T
     * @param array<string, array<string, T>> $tables
     * @return list<string>
     */
    public function columns(string $tableName, array $tables): array
    {
        foreach ($tables as $candidate => $context) {
            if (strcasecmp($candidate, $tableName) !== 0) {
                continue;
            }
            $columns = $context['columns'] ?? null;
            if (!is_array($columns)) {
                return [];
            }

            $names = [];
            foreach ($columns as $column) {
                if (is_string($column)) {
                    $names[] = $column;
                }
            }

            return $names;
        }

        return [];
    }
}
