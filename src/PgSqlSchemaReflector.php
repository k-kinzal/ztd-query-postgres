<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres;

use ZtdQuery\Connection\ConnectionInterface;
use ZtdQuery\Platform\SchemaReflector;

/**
 * Fetches PostgreSQL schema information via information_schema queries.
 *
 * Reconstructs CREATE TABLE statements from pg_catalog/information_schema
 * since PostgreSQL has no SHOW CREATE TABLE equivalent.
 */
final class PgSqlSchemaReflector implements SchemaReflector
{
    private ConnectionInterface $connection;

    public function __construct(ConnectionInterface $connection)
    {
        $this->connection = $connection;
    }

    /**
     * {@inheritDoc}
     */
    public function getCreateStatement(string $tableName): ?string
    {
        return $this->buildCreateTableSql($tableName);
    }

    /**
     * {@inheritDoc}
     */
    public function reflectAll(): array
    {
        $stmt = $this->connection->query(
            "SELECT table_name FROM information_schema.tables "
            . "WHERE table_schema = current_schema() AND table_type = 'BASE TABLE' "
            . "ORDER BY table_name"
        );
        if ($stmt === false) {
            return [];
        }

        $tables = $stmt->fetchAll();
        $result = [];

        foreach ($tables as $row) {
            $tableName = $row['table_name'] ?? null;
            if (!is_string($tableName) || $tableName === '') {
                continue;
            }

            $createSql = $this->buildCreateTableSql($tableName);
            if ($createSql !== null) {
                $result[$tableName] = $createSql;
            }
        }

        return $result;
    }

    private function buildCreateTableSql(string $tableName): ?string
    {
        $stmt = $this->connection->query(
            "SELECT column_name, data_type, character_maximum_length, "
            . "numeric_precision, numeric_scale, is_nullable, column_default, "
            . "udt_name "
            . "FROM information_schema.columns "
            . "WHERE table_schema = current_schema() AND table_name = '" . str_replace("'", "''", $tableName) . "' "
            . "ORDER BY ordinal_position"
        );
        if ($stmt === false) {
            return null;
        }

        $columns = $stmt->fetchAll();
        if ($columns === []) {
            return null;
        }

        $columnDefs = [];
        foreach ($columns as $col) {
            $columnDefs[] = $this->buildColumnDefinition($col);
        }

        $pkStmt = $this->connection->query(
            "SELECT kcu.column_name "
            . "FROM information_schema.table_constraints tc "
            . "JOIN information_schema.key_column_usage kcu "
            . "  ON tc.constraint_name = kcu.constraint_name "
            . "  AND tc.table_schema = kcu.table_schema "
            . "WHERE tc.table_schema = current_schema() "
            . "  AND tc.table_name = '" . str_replace("'", "''", $tableName) . "' "
            . "  AND tc.constraint_type = 'PRIMARY KEY' "
            . "ORDER BY kcu.ordinal_position"
        );

        $primaryKeyCols = [];
        if ($pkStmt !== false) {
            $pkRows = $pkStmt->fetchAll();
            foreach ($pkRows as $pkRow) {
                $colName = $pkRow['column_name'] ?? null;
                if (is_string($colName)) {
                    $primaryKeyCols[] = '"' . $colName . '"';
                }
            }
        }

        $uniqueStmt = $this->connection->query(
            "SELECT tc.constraint_name, kcu.column_name "
            . "FROM information_schema.table_constraints tc "
            . "JOIN information_schema.key_column_usage kcu "
            . "  ON tc.constraint_name = kcu.constraint_name "
            . "  AND tc.table_schema = kcu.table_schema "
            . "WHERE tc.table_schema = current_schema() "
            . "  AND tc.table_name = '" . str_replace("'", "''", $tableName) . "' "
            . "  AND tc.constraint_type = 'UNIQUE' "
            . "ORDER BY tc.constraint_name, kcu.ordinal_position"
        );

        /** @var array<string, list<string>> $uniqueConstraints */
        $uniqueConstraints = [];
        if ($uniqueStmt !== false) {
            $uniqueRows = $uniqueStmt->fetchAll();
            foreach ($uniqueRows as $uRow) {
                $constraintName = $uRow['constraint_name'] ?? '';
                $colName = $uRow['column_name'] ?? null;
                if (is_string($constraintName) && is_string($colName)) {
                    $uniqueConstraints[$constraintName][] = '"' . $colName . '"';
                }
            }
        }

        $parts = $columnDefs;

        if ($primaryKeyCols !== []) {
            $parts[] = 'PRIMARY KEY (' . implode(', ', $primaryKeyCols) . ')';
        }

        foreach ($uniqueConstraints as $constraintName => $constraintCols) {
            $parts[] = 'CONSTRAINT "' . $constraintName . '" UNIQUE (' . implode(', ', $constraintCols) . ')';
        }

        return 'CREATE TABLE "' . $tableName . '" (' . "\n  " . implode(",\n  ", $parts) . "\n)";
    }

    /**
     * @param array<string, mixed> $col
     */
    private function buildColumnDefinition(array $col): string
    {
        $columnName = isset($col['column_name']) && is_string($col['column_name']) ? $col['column_name'] : '';
        $name = '"' . $columnName . '"';
        $dataType = strtoupper(isset($col['data_type']) && is_string($col['data_type']) ? $col['data_type'] : 'TEXT');
        $udtName = strtoupper(isset($col['udt_name']) && is_string($col['udt_name']) ? $col['udt_name'] : '');

        $typeSql = $this->buildTypeSql($dataType, $udtName, $col);

        $def = "$name $typeSql";

        $isNullable = $col['is_nullable'] ?? 'YES';
        if ($isNullable === 'NO') {
            $def .= ' NOT NULL';
        }

        $default = $col['column_default'] ?? null;
        if (is_string($default) && $default !== '') {
            $def .= ' DEFAULT ' . $default;
        }

        return $def;
    }

    /**
     * @param array<string, mixed> $col
     */
    private function buildTypeSql(string $dataType, string $udtName, array $col): string
    {
        return match ($dataType) {
            'CHARACTER VARYING' => $this->buildVarcharType($col),
            'CHARACTER' => $this->buildCharType($col),
            'NUMERIC' => $this->buildNumericType($col),
            'INTEGER' => 'INTEGER',
            'SMALLINT' => 'SMALLINT',
            'BIGINT' => 'BIGINT',
            'REAL' => 'REAL',
            'DOUBLE PRECISION' => 'DOUBLE PRECISION',
            'BOOLEAN' => 'BOOLEAN',
            'DATE' => 'DATE',
            'TIMESTAMP WITHOUT TIME ZONE' => 'TIMESTAMP',
            'TIMESTAMP WITH TIME ZONE' => 'TIMESTAMPTZ',
            'TIME WITHOUT TIME ZONE' => 'TIME',
            'TIME WITH TIME ZONE' => 'TIMETZ',
            'TEXT' => 'TEXT',
            'BYTEA' => 'BYTEA',
            'JSON' => 'JSON',
            'JSONB' => 'JSONB',
            'UUID' => 'UUID',
            'USER-DEFINED' => $this->resolveUserDefinedType($udtName),
            'ARRAY' => $this->resolveArrayType($udtName),
            default => $dataType,
        };
    }

    /**
     * @param array<string, mixed> $col
     */
    private function buildVarcharType(array $col): string
    {
        $maxLen = $col['character_maximum_length'] ?? null;
        if (is_int($maxLen) || (is_string($maxLen) && ctype_digit($maxLen))) {
            return "VARCHAR($maxLen)";
        }

        return 'VARCHAR';
    }

    /**
     * @param array<string, mixed> $col
     */
    private function buildCharType(array $col): string
    {
        $maxLen = $col['character_maximum_length'] ?? null;
        if (is_int($maxLen) || (is_string($maxLen) && ctype_digit($maxLen))) {
            return "CHAR($maxLen)";
        }

        return 'CHAR(1)';
    }

    /**
     * @param array<string, mixed> $col
     */
    private function buildNumericType(array $col): string
    {
        $precision = $col['numeric_precision'] ?? null;
        $scale = $col['numeric_scale'] ?? null;

        if ($precision !== null && $scale !== null) {
            $p = is_int($precision) ? $precision : (is_string($precision) ? (int) $precision : 0);
            $s = is_int($scale) ? $scale : (is_string($scale) ? (int) $scale : 0);

            return "NUMERIC($p,$s)";
        }
        if ($precision !== null) {
            $p = is_int($precision) ? $precision : (is_string($precision) ? (int) $precision : 0);

            return "NUMERIC($p)";
        }

        return 'NUMERIC';
    }

    private function resolveUserDefinedType(string $udtName): string
    {
        return match ($udtName) {
            'CITEXT' => 'CITEXT',
            'HSTORE' => 'HSTORE',
            'LTREE' => 'LTREE',
            default => $udtName !== '' ? $udtName : 'TEXT',
        };
    }

    private function resolveArrayType(string $udtName): string
    {
        if (str_starts_with($udtName, '_')) {
            $baseType = strtoupper(substr($udtName, 1));

            return $baseType . '[]';
        }

        return $udtName . '[]';
    }
}
