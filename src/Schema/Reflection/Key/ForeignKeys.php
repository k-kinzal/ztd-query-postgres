<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Schema\Reflection\Key;

use ZtdQuery\Connection\StatementInterface;

/**
 * Reconstructs ordered composite foreign keys and referential actions.
 *
 * @visibility root
 */
final class ForeignKeys
{
    /**
     * Groups ordered catalog rows into complete foreign-key declarations.
     * @return list<string>
     */
    public function definitions(StatementInterface|false $statement): array
    {
        if ($statement === false) {
            return [];
        }
        /**
         * @var array<string, array{columns: list<string>, table: string, referencedColumns: list<string>, onUpdate: string, onDelete: string}> $keys
         */
        $keys = [];
        foreach ($statement->fetchAll() as $row) {
            $key = (new ForeignKeyRow())->parse($row);
            if ($key === null) {
                continue;
            }
            $name = $key['name'];
            if (!isset($keys[$name])) {
                $keys[$name] = ['columns' => [], 'table' => $key['table'], 'referencedColumns' => [], 'onUpdate' => $key['onUpdate'], 'onDelete' => $key['onDelete']];
            }
            $keys[$name]['columns'][] = '"' . $key['column'] . '"';
            $keys[$name]['referencedColumns'][] = '"' . $key['referencedColumn'] . '"';
        }
        $sql = [];
        foreach ($keys as $name => $key) {
            $sql[] = $this->render($name, $key);
        }
        return $sql;
    }

    /**
     * Renders a validated composite foreign-key constraint and its actions.
     * @param array{columns: list<string>, table: string, referencedColumns: list<string>, onUpdate: string, onDelete: string} $foreignKey
     */
    public function render(string $constraintName, array $foreignKey): string
    {
        return 'CONSTRAINT "' . $constraintName . '" FOREIGN KEY ('
            . implode(', ', $foreignKey['columns']) . ') REFERENCES "'
            . $foreignKey['table'] . '" (' . implode(', ', $foreignKey['referencedColumns']) . ')'
            . ' ON UPDATE ' . $foreignKey['onUpdate']
            . ' ON DELETE ' . $foreignKey['onDelete'];
    }
}
