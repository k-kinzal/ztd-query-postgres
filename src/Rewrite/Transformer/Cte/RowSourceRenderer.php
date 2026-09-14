<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Rewrite\Transformer\Cte;

use RuntimeException;
use ZtdQuery\Platform\CastRenderer;
use ZtdQuery\Platform\IdentifierQuoter;
use ZtdQuery\Platform\Postgres\Rewrite\GeneratedColumn\PgSqlGeneratedColumnProjector;
use ZtdQuery\Platform\ValueRenderer;
use ZtdQuery\Schema\ColumnDeclaration;
use ZtdQuery\Schema\ColumnTypeFamily;

/**
 * Row source renderer operations for PostgreSQL cte.
 *
 * @visibility root
 */
final class RowSourceRenderer
{
    private readonly CastRenderer $castRenderer;
    private readonly PgSqlGeneratedColumnProjector $generatedColumnProjector;
    private readonly IdentifierQuoter $quoter;
    private readonly ValueRenderer $valueRenderer;

    /**
     * Supplies the dependencies used by this RowSourceRenderer.
     */
    public function __construct(CastRenderer $castRenderer, PgSqlGeneratedColumnProjector $generatedColumnProjector, IdentifierQuoter $quoter, ValueRenderer $valueRenderer)
    {
        $this->castRenderer = $castRenderer;
        $this->generatedColumnProjector = $generatedColumnProjector;
        $this->quoter = $quoter;
        $this->valueRenderer = $valueRenderer;
    }

    /**
     * @template T
     * @param array<int, array<string, T>> $rows
     * @param array<int, string> $columns
     * @param array<string, ColumnDeclaration> $columnTypes
     * @param array<string, string> $generatedExpressions
     * @throws RuntimeException
     */
    public function generateCte(
        string $tableName,
        array $rows,
        array $columns,
        array $columnTypes,
        array $generatedExpressions,
    ): string {
        $quotedTable = $this->quoter->quote($tableName);

        if ($columns !== []) {
            return $this->wrapCte($quotedTable, $this->declaredSource($rows, $columns, $columnTypes), $columns, $generatedExpressions);
        }

        if ($rows === []) {
            throw new RuntimeException("Cannot shadow table '$tableName' with empty data (columns unknown).");
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
     * @template T
     * @param array<int, array<string, T>> $rows
     * @param array<int, string> $columns
     * @param array<string, ColumnDeclaration> $columnTypes
     */
    public function generateMultiRowSource(
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
    public function wrapCte(
        string $quotedTable,
        string $baseSql,
        array $columns,
        array $generatedExpressions,
    ): string {
        $sql = $this->generatedColumnProjector->project($baseSql, $columns, $generatedExpressions);

        return "$quotedTable AS MATERIALIZED ($sql)";
    }

    /**
     * Format value.
     * @template T
     * @param T $val
     */
    public function formatValue(mixed $val, ?ColumnDeclaration $colType = null): string
    {
        return $this->valueRenderer->renderValue($val, $colType);
    }

    /**
     * Render fallback null cast.
     */
    public function renderFallbackNullCast(): string
    {
        return $this->castRenderer->renderNullCast(
            new ColumnDeclaration(ColumnTypeFamily::TEXT, 'TEXT'),
        );
    }
    /**
     * Renders an empty, single-row or multi-row source using declared column types.
     * @template T
     * @param array<int, array<string, T>> $rows
     * @param array<int, string> $columns
     * @param array<string, ColumnDeclaration> $columnTypes
     */
    public function declaredSource(array $rows, array $columns, array $columnTypes): string
    {
        if ($rows === []) {
            $selects = [];
            foreach ($columns as $col) {
                $type = $columnTypes[$col] ?? null;
                $nullCast = $type !== null ? $this->castRenderer->renderNullCast($type) : $this->renderFallbackNullCast();
                $selects[] = "$nullCast AS " . $this->quoter->quote($col);
            }
            return 'SELECT ' . implode(', ', $selects) . ' WHERE FALSE';
        }
        if (count($rows) === 1) {
            $selects = [];
            $row = $rows[0];
            foreach ($columns as $col) {
                $value = $this->formatValue($row[$col] ?? null, $columnTypes[$col] ?? null);
                $selects[] = "$value AS " . $this->quoter->quote($col);
            }
            return 'SELECT ' . implode(', ', $selects);
        }
        return $this->generateMultiRowSource($rows, $columns, $columnTypes);
    }
}
