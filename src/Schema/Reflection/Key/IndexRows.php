<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Schema\Reflection\Key;

use ZtdQuery\Schema\Key\PartialUniqueIndex;

/**
 * Collects composite unique-index metadata before filtering unsupported expression indexes.
 *
 * @visibility root
 */
final class IndexRows
{
    /**
     * @var array<string, list<string>>
     */
    private array $uniqueConstraints = [];
    /**
     * @var array<string, array{columns: list<string>, predicate: string}>
     */
    private array $partialIndexes = [];
    /**
     * @var array<string, string>
     */
    private array $invalidIndexes = [];

    /**
     * Adds one catalog row while retaining invalid-index markers across all its columns.
     * @template T
     * @param array<string, T> $uRow
     */
    public function append(array $uRow): void
    {
        $constraintName = $uRow['constraint_name'] ?? '';
        $colName = $uRow['column_name'] ?? null;
        $predicate = $uRow['predicate'] ?? null;
        if (!is_string($constraintName)) {
            return;
        }
        if ($constraintName === '') {
            return;
        }
        if (!is_string($colName)) {
            $this->invalidIndexes[$constraintName] = $constraintName;
            return;
        }
        if (!is_string($predicate) || trim($predicate) === '') {
            $this->uniqueConstraints[$constraintName][] = '"' . $colName . '"';

            return;
        }
        if (!isset($this->partialIndexes[$constraintName])) {
            $this->partialIndexes[$constraintName] = ['columns' => [$colName], 'predicate' => $predicate];

            return;
        }
        $this->partialIndexes[$constraintName]['columns'][] = $colName;
    }

    /**
     * Excludes expression indexes and produces both SQL and partial-index metadata.
     */
    public function definitions(): IndexDefinitions
    {
        $unique = $this->uniqueConstraints;
        $partial = $this->partialIndexes;
        foreach ($this->invalidIndexes as $name) {
            unset($unique[$name], $partial[$name]);
        }
        $sql = [];
        foreach ($unique as $name => $columns) {
            $sql[] = 'CONSTRAINT "' . $name . '" UNIQUE (' . implode(', ', $columns) . ')';
        }
        $indexes = [];
        foreach ($partial as $name => $index) {
            if ($index['columns'] === []) {
                continue;
            }
            $indexes[$name] = new PartialUniqueIndex($name, $index['columns'], $index['predicate']);
        }
        return new IndexDefinitions($sql, $indexes);
    }
}
