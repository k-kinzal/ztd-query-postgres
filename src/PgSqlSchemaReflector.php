<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres;

use ZtdQuery\Connection\ConnectionInterface;
use ZtdQuery\Platform\SchemaReflector;
use ZtdQuery\Platform\ViewReflector;
use ZtdQuery\Schema\PartialUniqueIndex;
use ZtdQuery\Schema\ViewDefinition;

/**
 * Fetches PostgreSQL schema information via information_schema queries.
 *
 * Reconstructs CREATE TABLE statements from pg_catalog/information_schema
 * since PostgreSQL has no SHOW CREATE TABLE equivalent.
 */
final class PgSqlSchemaReflector implements SchemaReflector, ViewReflector
{
    private ConnectionInterface $connection;

    /** @var array<string, array<string, PartialUniqueIndex>> */
    private array $partialUniqueIndexes = [];

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

    /** @return array<string, array<string, PartialUniqueIndex>> */
    public function partialUniqueIndexes(): array
    {
        return $this->partialUniqueIndexes;
    }

    /** {@inheritDoc} */
    public function reflectViews(): array
    {
        $stmt = $this->connection->query(
            'SELECT viewname, definition FROM pg_views WHERE schemaname = current_schema() ORDER BY viewname',
        );
        if ($stmt === false) {
            return [];
        }

        $definitions = [];
        foreach ($stmt->fetchAll() as $row) {
            $viewName = $row['viewname'] ?? null;
            $query = $row['definition'] ?? null;
            if (!is_string($viewName) || $viewName === '' || !is_string($query) || trim($query) === '') {
                continue;
            }
            $definitions[$viewName] = (new PgSqlViewDefinitionParser())->fromQuery($query);
        }

        return $definitions;
    }

    private function buildCreateTableSql(string $tableName): ?string
    {
        $escapedTableName = str_replace("'", "''", $tableName);
        $stmt = $this->connection->query(
            "SELECT column_name, data_type, character_maximum_length, "
            . "numeric_precision, numeric_scale, is_nullable, column_default, "
            . "udt_name, domain_schema, domain_name, is_identity, identity_generation, "
            . "is_generated, generation_expression "
            . "FROM information_schema.columns "
            . "WHERE table_schema = current_schema() AND table_name = '" . $escapedTableName . "' "
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
            . "  AND tc.table_name = '" . $escapedTableName . "' "
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
            'SELECT index_relation.relname AS constraint_name, attribute.attname AS column_name, '
            . 'pg_get_expr(index_metadata.indpred, index_metadata.indrelid) AS predicate '
            . 'FROM pg_catalog.pg_class table_relation '
            . 'JOIN pg_catalog.pg_namespace namespace ON namespace.oid = table_relation.relnamespace '
            . 'JOIN pg_catalog.pg_index index_metadata ON index_metadata.indrelid = table_relation.oid '
            . 'JOIN pg_catalog.pg_class index_relation ON index_relation.oid = index_metadata.indexrelid '
            . 'JOIN LATERAL unnest(index_metadata.indkey) WITH ORDINALITY key_column(attnum, ordinality) '
            . '  ON key_column.ordinality <= index_metadata.indnkeyatts '
            . 'LEFT JOIN pg_catalog.pg_attribute attribute '
            . '  ON attribute.attrelid = table_relation.oid AND attribute.attnum = key_column.attnum '
            . 'WHERE namespace.nspname = current_schema() '
            . "  AND table_relation.relname = '" . $escapedTableName . "' "
            . '  AND index_metadata.indisunique '
            . '  AND index_metadata.indisvalid '
            . '  AND NOT index_metadata.indisprimary '
            . 'ORDER BY index_relation.relname, key_column.ordinality'
        );

        /** @var array<string, list<string>> $uniqueConstraints */
        $uniqueConstraints = [];
        /** @var array<string, array{columns: list<string>, predicate: string}> $partialIndexes */
        $partialIndexes = [];
        /** @var array<string, string> $invalidIndexes */
        $invalidIndexes = [];
        if ($uniqueStmt !== false) {
            $uniqueRows = $uniqueStmt->fetchAll();
            foreach ($uniqueRows as $uRow) {
                $constraintName = $uRow['constraint_name'] ?? '';
                $colName = $uRow['column_name'] ?? null;
                $predicate = $uRow['predicate'] ?? null;
                if (!is_string($constraintName)) {
                    continue;
                }
                if ($constraintName === '') {
                    continue;
                }
                if (!is_string($colName)) {
                    $invalidIndexes[$constraintName] = $constraintName;
                    continue;
                }
                if (!is_string($predicate) || trim($predicate) === '') {
                    $uniqueConstraints[$constraintName][] = '"' . $colName . '"';

                    continue;
                }
                if (!isset($partialIndexes[$constraintName])) {
                    $partialIndexes[$constraintName] = ['columns' => [$colName], 'predicate' => $predicate];

                    continue;
                }
                $partialIndexes[$constraintName]['columns'][] = $colName;
            }
        }
        foreach ($invalidIndexes as $invalidIndex) {
            unset($uniqueConstraints[$invalidIndex], $partialIndexes[$invalidIndex]);
        }
        $this->partialUniqueIndexes[$tableName] = [];
        foreach ($partialIndexes as $name => $index) {
            if ($index['columns'] === []) {
                continue;
            }
            $this->partialUniqueIndexes[$tableName][$name] = new PartialUniqueIndex(
                $name,
                $index['columns'],
                $index['predicate'],
            );
        }

        $foreignKeyStmt = $this->connection->query(
            "SELECT fk.constraint_name, fk.column_name, "
            . "pk.table_name AS foreign_table_name, pk.column_name AS foreign_column_name, "
            . "rc.update_rule, rc.delete_rule "
            . "FROM information_schema.referential_constraints rc "
            . "JOIN information_schema.key_column_usage fk "
            . "  ON fk.constraint_catalog = rc.constraint_catalog "
            . "  AND fk.constraint_schema = rc.constraint_schema "
            . "  AND fk.constraint_name = rc.constraint_name "
            . "JOIN information_schema.key_column_usage pk "
            . "  ON pk.constraint_catalog = rc.unique_constraint_catalog "
            . "  AND pk.constraint_schema = rc.unique_constraint_schema "
            . "  AND pk.constraint_name = rc.unique_constraint_name "
            . "  AND pk.ordinal_position = fk.position_in_unique_constraint "
            . "WHERE fk.table_schema = current_schema() "
            . "  AND fk.table_name = '" . $escapedTableName . "' "
            . "ORDER BY fk.constraint_name, fk.ordinal_position"
        );

        /** @var array<string, array{columns: list<string>, table: string, referencedColumns: list<string>, onUpdate: string, onDelete: string}> $foreignKeys */
        $foreignKeys = [];
        if ($foreignKeyStmt !== false) {
            foreach ($foreignKeyStmt->fetchAll() as $foreignKeyRow) {
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
                    continue;
                }
                if (!isset($foreignKeys[$constraintName])) {
                    $foreignKeys[$constraintName] = [
                        'columns' => ['"' . $columnName . '"'],
                        'table' => $foreignTable,
                        'referencedColumns' => ['"' . $foreignColumn . '"'],
                        'onUpdate' => $onUpdate,
                        'onDelete' => $onDelete,
                    ];

                    continue;
                }
                $foreignKeys[$constraintName]['columns'][] = '"' . $columnName . '"';
                $foreignKeys[$constraintName]['referencedColumns'][] = '"' . $foreignColumn . '"';
            }
        }

        $parts = $columnDefs;

        if ($primaryKeyCols !== []) {
            $parts[] = 'PRIMARY KEY (' . implode(', ', $primaryKeyCols) . ')';
        }

        foreach ($uniqueConstraints as $constraintName => $constraintCols) {
            $parts[] = 'CONSTRAINT "' . $constraintName . '" UNIQUE (' . implode(', ', $constraintCols) . ')';
        }

        foreach ($foreignKeys as $constraintName => $foreignKey) {
            $parts[] = 'CONSTRAINT "' . $constraintName . '" FOREIGN KEY ('
                . implode(', ', $foreignKey['columns']) . ') REFERENCES "'
                . $foreignKey['table'] . '" (' . implode(', ', $foreignKey['referencedColumns']) . ')'
                . ' ON UPDATE ' . $foreignKey['onUpdate']
                . ' ON DELETE ' . $foreignKey['onDelete'];
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

        $typeSql = $this->domainTypeSql($col) ?? $this->buildTypeSql($dataType, $udtName, $col);

        $def = "$name $typeSql";

        $isNullable = $col['is_nullable'] ?? 'YES';
        if ($isNullable === 'NO') {
            $def .= ' NOT NULL';
        }

        $generationExpression = $col['generation_expression'] ?? null;
        if (($col['is_generated'] ?? 'NEVER') === 'ALWAYS'
            && is_string($generationExpression)
            && trim($generationExpression) !== ''
        ) {
            $def .= ' GENERATED ALWAYS AS (' . $generationExpression . ') STORED';
        } elseif (($col['is_identity'] ?? 'NO') === 'YES') {
            $identityGeneration = ($col['identity_generation'] ?? 'BY DEFAULT') === 'ALWAYS'
                ? 'ALWAYS'
                : 'BY DEFAULT';
            $def .= " GENERATED $identityGeneration AS IDENTITY";
        } else {
            $default = $col['column_default'] ?? null;
            if (is_string($default) && $default !== '') {
                $def .= ' DEFAULT ' . $default;
            }
        }

        return $def;
    }

    /**
     * @param array<string, mixed> $col
     */
    private function domainTypeSql(array $col): ?string
    {
        $domainName = $col['domain_name'] ?? null;
        if (!is_string($domainName)) {
            return null;
        }
        if ($domainName === '') {
            return null;
        }

        $quoter = new PgSqlIdentifierQuoter();
        $domainSchema = $col['domain_schema'] ?? null;
        if (!is_string($domainSchema)) {
            return $quoter->quote($domainName);
        }
        if ($domainSchema === '') {
            return $quoter->quote($domainName);
        }

        return $quoter->quote($domainSchema) . '.' . $quoter->quote($domainName);
    }

    /**
     * @param array<string, mixed> $col
     */
    private function buildTypeSql(string $dataType, string $udtName, array $col): string
    {
        return match ($dataType) {
            'CHARACTER VARYING' => $this->buildVarcharType($col),
            'CHARACTER' => $this->buildCharType($col),
            'BIT', 'BIT VARYING' => $this->buildBitType($dataType, $col),
            'NUMERIC' => $this->buildNumericType($col),
            'TIMESTAMP WITHOUT TIME ZONE' => 'TIMESTAMP',
            'TIMESTAMP WITH TIME ZONE' => 'TIMESTAMPTZ',
            'TIME WITHOUT TIME ZONE' => 'TIME',
            'TIME WITH TIME ZONE' => 'TIMETZ',
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
    private function buildBitType(string $dataType, array $col): string
    {
        $maxLen = $col['character_maximum_length'] ?? null;
        if (is_int($maxLen) || (is_string($maxLen) && ctype_digit($maxLen))) {
            return "$dataType($maxLen)";
        }

        return $dataType;
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
