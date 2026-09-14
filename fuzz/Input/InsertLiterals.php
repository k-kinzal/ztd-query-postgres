<?php

declare(strict_types=1);

namespace Fuzz\Input;

use ZtdQuery\Schema\TableDefinition;

/**
 * Creates reproducible PostgreSQL pipeline fixtures.
 */
final class InsertLiterals
{
    /**
     * Supplies a seeded value generator.
     */
    public function __construct(private readonly \Faker\Generator $faker)
    {
    }

    /**
     * Build a VALUES clause with placeholder literals for all columns.
     */
    public function buildInsertValues(TableDefinition $definition): string
    {
        $values = [];
        foreach ($definition->columns as $col) {
            $type = strtoupper($definition->columnTypes[$col] ?? 'TEXT');
            $baseType = preg_replace('/\(.*\)/', '', $type);
            $baseType = trim($baseType ?? $type);
            $values[] = match (true) {
                in_array($baseType, ['INT', 'INT2', 'INT4', 'INT8', 'INTEGER', 'SMALLINT', 'BIGINT', 'SERIAL', 'SMALLSERIAL', 'BIGSERIAL'], true) => (string) $this->faker->numberBetween(1, 9999),
                in_array($baseType, ['REAL', 'FLOAT4', 'DOUBLE PRECISION', 'FLOAT8', 'DECIMAL', 'NUMERIC'], true) => (string) round($this->faker->randomFloat(2, 0, 999), 2),
                in_array($baseType, ['BOOLEAN', 'BOOL'], true) => $this->faker->boolean() ? 'TRUE' : 'FALSE',
                default => "'" . str_replace("'", "''", $this->faker->word()) . "'",
            };
        }
        return implode(', ', $values);
    }
}
