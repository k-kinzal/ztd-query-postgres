<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Transformer;

use ZtdQuery\Platform\Postgres\PgSqlCastRenderer;
use ZtdQuery\Platform\Postgres\PgSqlIdentifierQuoter;
use ZtdQuery\Platform\Postgres\PgSqlTableSampleRewriter;
use ZtdQuery\Platform\CastRenderer;
use ZtdQuery\Platform\IdentifierQuoter;
use ZtdQuery\Platform\ValueRenderer;
use ZtdQuery\Platform\Postgres\PgSqlCteShadowComposer;
use ZtdQuery\Platform\Postgres\PgSqlGeneratedColumnProjector;
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
    private ValueRenderer $valueRenderer;
    private PgSqlCteShadowComposer $cteComposer;
    private PgSqlGeneratedColumnProjector $generatedColumnProjector;
    private PgSqlTableSampleRewriter $tableSampleRewriter;

    public function __construct(
        ?CastRenderer $castRenderer = null,
        ?IdentifierQuoter $quoter = null,
        ?ValueRenderer $valueRenderer = null,
    ) {
        $this->castRenderer = $castRenderer ?? new PgSqlCastRenderer();
        $this->quoter = $quoter ?? new PgSqlIdentifierQuoter();
        $this->valueRenderer = $valueRenderer ?? new \ZtdQuery\Platform\Postgres\PgSqlValueRenderer($this->castRenderer);
        $this->cteComposer = new PgSqlCteShadowComposer();
        $this->generatedColumnProjector = new PgSqlGeneratedColumnProjector();
        $this->tableSampleRewriter = new PgSqlTableSampleRewriter();
    }

    /**
     * {@inheritDoc}
     */
    public function transform(string $sql, array $tables): string
    {
        $sql = $this->tableSampleRewriter->rewrite($sql, $tables);
        $ctes = [];
        foreach ($tables as $tableName => $tableContext) {
            if (isset($tableContext['viewSql'])) {
                $ctes[$tableName] = $this->quoter->quote($tableName) . " AS MATERIALIZED ({$tableContext['viewSql']})";
                continue;
            }
            if (isset($tableContext['sourceSql'])) {
                $ctes[$tableName] = $this->quoter->quote($tableName) . " AS MATERIALIZED ({$tableContext['sourceSql']})";
                continue;
            }

            $rows = $tableContext['rows'];
            $columns = $tableContext['columns'];
            /** @var array<string, ColumnType> $columnTypes */
            $columnTypes = $tableContext['columnTypes'];
            $generatedExpressions = $tableContext['generatedExpressions'] ?? [];

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

            $ctes[$tableName] = $this->generateCte(
                $tableName,
                $rows,
                $columns,
                $columnTypes,
                $generatedExpressions,
            );
        }

        return $this->cteComposer->compose($sql, $ctes);
    }

    /**
     * @param array<int, array<string, mixed>> $rows
     * @param array<int, string> $columns
     * @param array<string, ColumnType> $columnTypes
     * @param array<string, string> $generatedExpressions
     */
    private function generateCte(
        string $tableName,
        array $rows,
        array $columns,
        array $columnTypes,
        array $generatedExpressions,
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

                return $this->wrapCte(
                    $quotedTable,
                    'SELECT ' . implode(', ', $selects) . ' WHERE FALSE',
                    $columns,
                    $generatedExpressions,
                );
            }

            if (count($rows) === 1) {
                $selects = [];
                $row = $rows[0];
                foreach ($columns as $col) {
                    $colType = $columnTypes[$col] ?? null;
                    $valStr = $this->formatValue($row[$col] ?? null, $colType);
                    $selects[] = "$valStr AS " . $this->quoter->quote($col);
                }

                return $this->wrapCte(
                    $quotedTable,
                    'SELECT ' . implode(', ', $selects),
                    $columns,
                    $generatedExpressions,
                );
            }

            $baseSql = $this->generateMultiRowSource($rows, $columns, $columnTypes);

            return $this->wrapCte($quotedTable, $baseSql, $columns, $generatedExpressions);
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

        return $this->wrapCte($quotedTable, $union, array_keys($rows[0]), $generatedExpressions);
    }

    /**
     * @param array<int, array<string, mixed>> $rows
     * @param array<int, string> $columns
     * @param array<string, ColumnType> $columnTypes
     */
    private function generateMultiRowSource(
        array $rows,
        array $columns,
        array $columnTypes
    ): string {
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

        return "\n  SELECT * FROM (VALUES\n    $valuesClause\n  ) AS t($columnList)\n";
    }

    /**
     * @param array<int, string> $columns
     * @param array<string, string> $generatedExpressions
     */
    private function wrapCte(
        string $quotedTable,
        string $baseSql,
        array $columns,
        array $generatedExpressions,
    ): string {
        $sql = $this->generatedColumnProjector->project($baseSql, $columns, $generatedExpressions);

        return "$quotedTable AS MATERIALIZED ($sql)";
    }

    private function formatValue(mixed $val, ?ColumnType $colType = null): string
    {
        return $this->valueRenderer->renderValue($val, $colType);
    }

    private function renderFallbackNullCast(): string
    {
        return $this->castRenderer->renderNullCast(
            new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
        );
    }

}
