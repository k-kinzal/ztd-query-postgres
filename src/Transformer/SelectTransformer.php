<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Transformer;

use ZtdQuery\Platform\Postgres\PgSqlCastRenderer;
use ZtdQuery\Platform\Postgres\PgSqlIdentifierQuoter;
use ZtdQuery\Platform\CastRenderer;
use ZtdQuery\Platform\IdentifierQuoter;
use ZtdQuery\Rewrite\SqlTransformer;
use ZtdQuery\Schema\ColumnType;
use ZtdQuery\Schema\ColumnTypeFamily;

/**
 * Applies CTE shadowing to SELECT statements for PostgreSQL.
 *
 * Key differences from MySQL SelectTransformer:
 * - Uses double-quote identifiers ("table") instead of backticks
 * - Uses AS MATERIALIZED for CTE definition (PG 12+ inline prevention)
 * - Uses VALUES clause for multi-row CTEs instead of UNION ALL chains
 * - Uses WHERE FALSE for empty CTEs instead of FROM DUAL WHERE 0
 * - Uses PostgreSQL CAST types (INTEGER, TEXT, BOOLEAN, etc.)
 */
final class SelectTransformer implements SqlTransformer
{
    private CastRenderer $castRenderer;
    private IdentifierQuoter $quoter;

    public function __construct(?CastRenderer $castRenderer = null, ?IdentifierQuoter $quoter = null)
    {
        $this->castRenderer = $castRenderer ?? new PgSqlCastRenderer();
        $this->quoter = $quoter ?? new PgSqlIdentifierQuoter();
    }

    /**
     * {@inheritDoc}
     */
    public function transform(string $sql, array $tables): string
    {
        $ctes = [];
        foreach ($tables as $tableName => $tableContext) {
            if (stripos($sql, $tableName) === false) {
                continue;
            }

            $rows = $tableContext['rows'];
            $columns = $tableContext['columns'];
            /** @var array<string, ColumnType> $columnTypes */
            $columnTypes = $tableContext['columnTypes'];

            if ($columns === [] && $rows !== []) {
                $columns = array_keys($rows[0]);
                foreach ($rows as $row) {
                    foreach (array_keys($row) as $column) {
                        if (!in_array($column, $columns, true)) {
                            $columns[] = $column;
                        }
                    }
                }
            }

            if ($columns === [] && $rows === []) {
                continue;
            }

            $ctes[] = $this->generateCte($tableName, $rows, $columns, $columnTypes);
        }

        if ($ctes === []) {
            return $sql;
        }

        $cteString = implode(",\n", $ctes);
        $pattern = '/^(\s*(?:(?:\/\*.*?\*\/)|(?:--.*?\n)|(?:#.*?\n)|\s)*)WITH\b/is';
        if (preg_match($pattern, $sql) === 1) {
            return (string) preg_replace($pattern, '$1WITH ' . $cteString . ",\n", $sql, 1);
        }

        return "WITH $cteString\n$sql";
    }

    /**
     * @param array<int, array<string, mixed>> $rows
     * @param array<int, string> $columns
     * @param array<string, ColumnType> $columnTypes
     */
    private function generateCte(
        string $tableName,
        array $rows,
        array $columns,
        array $columnTypes
    ): string {
        $quotedTable = $this->quoter->quote($tableName);

        if ($columns !== []) {
            if ($rows === []) {
                $selects = [];
                foreach ($columns as $col) {
                    $type = $columnTypes[$col] ?? null;
                    $nullCast = $type !== null
                        ? $this->castRenderer->renderNullCast($type)
                        : $this->renderFallbackNullCast();
                    $selects[] = "$nullCast AS " . $this->quoter->quote($col);
                }

                return "$quotedTable AS MATERIALIZED (SELECT " . implode(', ', $selects) . ' WHERE FALSE)';
            }

            if (count($rows) === 1) {
                $selects = [];
                $row = $rows[0];
                foreach ($columns as $col) {
                    $colType = $columnTypes[$col] ?? null;
                    $valStr = $this->formatValue($row[$col] ?? null, $colType);
                    $selects[] = "$valStr AS " . $this->quoter->quote($col);
                }

                return "$quotedTable AS MATERIALIZED (SELECT " . implode(', ', $selects) . ')';
            }

            return $this->generateMultiRowCte($tableName, $rows, $columns, $columnTypes);
        }

        if ($rows === []) {
            throw new \RuntimeException("Cannot shadow table '$tableName' with empty data (columns unknown).");
        }

        $ctes = [];
        foreach ($rows as $row) {
            $selects = [];
            foreach ($row as $col => $val) {
                $colName = $col;
                $colType = $columnTypes[$colName] ?? null;
                $valStr = $this->formatValue($val, $colType);
                $selects[] = "$valStr AS " . $this->quoter->quote($colName);
            }
            $ctes[] = 'SELECT ' . implode(', ', $selects);
        }

        $union = implode(' UNION ALL ', $ctes);

        return "$quotedTable AS MATERIALIZED ($union)";
    }

    /**
     * @param array<int, array<string, mixed>> $rows
     * @param array<int, string> $columns
     * @param array<string, ColumnType> $columnTypes
     */
    private function generateMultiRowCte(
        string $tableName,
        array $rows,
        array $columns,
        array $columnTypes
    ): string {
        $quotedTable = $this->quoter->quote($tableName);

        $valueRows = [];
        foreach ($rows as $row) {
            $values = [];
            foreach ($columns as $col) {
                $colType = $columnTypes[$col] ?? null;
                $values[] = $this->formatValue($row[$col] ?? null, $colType);
            }
            $valueRows[] = '(' . implode(', ', $values) . ')';
        }

        $quotedColumns = [];
        foreach ($columns as $col) {
            $quotedColumns[] = $this->quoter->quote($col);
        }

        $valuesClause = implode(",\n    ", $valueRows);
        $columnList = implode(', ', $quotedColumns);

        return "$quotedTable AS MATERIALIZED (\n  SELECT * FROM (VALUES\n    $valuesClause\n  ) AS t($columnList)\n)";
    }

    private function formatValue(mixed $val, ?ColumnType $colType = null): string
    {
        if (is_null($val)) {
            return 'NULL';
        }

        if ($colType !== null) {
            if (is_string($val)) {
                $quotedVal = $this->quoteValue($val);
            } elseif (is_int($val) || is_float($val) || is_bool($val)) {
                $quotedVal = $this->quoteValue((string) $val);
            } else {
                $quotedVal = $this->quoteValue(serialize($val));
            }

            return $this->castRenderer->renderCast($quotedVal, $colType);
        }

        if (is_int($val)) {
            return $this->castRenderer->renderCast(
                (string) $val,
                new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
            );
        }
        if (is_string($val)) {
            return $this->castRenderer->renderCast(
                $this->quoteValue($val),
                new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            );
        }
        if (is_bool($val)) {
            return $val ? 'TRUE' : 'FALSE';
        }
        if (is_float($val)) {
            return (string) $val;
        }
        if (is_object($val) && method_exists($val, '__toString')) {
            return (string) $val;
        }
        throw new \RuntimeException('Unsupported value type for CTE shadowing.');
    }

    private function renderFallbackNullCast(): string
    {
        return $this->castRenderer->renderNullCast(
            new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
        );
    }

    private function quoteValue(string $val): string
    {
        return "'" . str_replace("'", "''", $val) . "'";
    }
}
