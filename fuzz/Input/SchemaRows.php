<?php

declare(strict_types=1);

namespace Fuzz\Input;

use ZtdQuery\Schema\TableDefinition;

/**
 * Creates reproducible PostgreSQL pipeline fixtures.
 */
final class SchemaRows
{
    /**
     * Generate random fixture rows for a table definition.
     *
     * @return array<int, array<string, int|float|string|bool>>
     */
    public function generateFixtureRows(TableDefinition $definition, int $count): array
    {
        $rows = [];
        for ($i = 0; $i < $count; $i++) {
            $row = [];
            foreach ($definition->columns as $col) {
                $type = strtoupper($definition->columnTypes[$col] ?? 'TEXT');
                $row[$col] = $this->generateValueForType($type, $i);
            }
            $rows[] = $row;
        }
        return $rows;
    }

    /**
     * Generate a random value appropriate for the given SQL type.
     */
    public function generateValueForType(string $type, int $seed): int|float|string|bool
    {
        $baseType = preg_replace('/\(.*\)/', '', $type);
        $baseType = trim($baseType ?? $type);
        return match (true) {
            in_array($baseType, ['INT', 'INT2', 'INT4', 'INT8', 'INTEGER', 'SMALLINT', 'BIGINT', 'SERIAL', 'SMALLSERIAL', 'BIGSERIAL'], true) => $seed + 1,
            in_array($baseType, ['REAL', 'FLOAT4', 'DOUBLE PRECISION', 'FLOAT8', 'DECIMAL', 'NUMERIC'], true) => round($seed + 0.5, 2),
            in_array($baseType, ['BOOLEAN', 'BOOL'], true) => $seed % 2 === 0,
            default => 'val_' . $seed,
        };
    }

    /**
     * Extract table name from a CREATE TABLE statement.
     */
    public function extractTableName(string $createSql): ?string
    {
        if (preg_match('/CREATE\s+(?:TEMPORARY\s+|TEMP\s+|UNLOGGED\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:(?:"([^"]+)"|([a-zA-Z_]\w*))\.)?(?:"([^"]+)"|([a-zA-Z_]\w*))/i', $createSql, $m) !== 1) {
            return null;
        }
        $quotedTable = $m[3] ?? '';
        return $quotedTable !== '' ? $quotedTable : $m[4] ?? null;
    }
}
