<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Transformer;

use ZtdQuery\Exception\UnsupportedSqlException;
use ZtdQuery\Platform\Postgres\PgSqlParser;
use ZtdQuery\Rewrite\SqlTransformer;

/**
 * Transforms DELETE statements into SELECT projections with CTE shadowing.
 *
 * PostgreSQL differences:
 * - USING clause instead of multi-table FROM/JOIN
 * - Double-quote identifiers
 */
final class DeleteTransformer implements SqlTransformer
{
    private PgSqlParser $parser;
    private SelectTransformer $selectTransformer;

    public function __construct(PgSqlParser $parser, SelectTransformer $selectTransformer)
    {
        $this->parser = $parser;
        $this->selectTransformer = $selectTransformer;
    }

    /**
     * {@inheritDoc}
     */
    public function transform(string $sql, array $tables): string
    {
        $targetTable = $this->parser->extractDeleteTable($sql);
        if ($targetTable === null) {
            throw new UnsupportedSqlException($sql, 'Cannot resolve DELETE target');
        }

        $columns = $tables[$targetTable]['columns'] ?? [];
        $projection = $this->buildProjection($sql, $targetTable, $columns);

        return $this->selectTransformer->transform($projection['sql'], $tables);
    }

    /**
     * Build a result-select SQL from a DELETE statement.
     *
     * @param array<int, string> $columns
     * @return array{sql: string, table: string, tables: array<string, array{alias: string}>}
     */
    public function buildProjection(string $sql, string $targetTable, array $columns): array
    {
        $alias = $this->parser->extractDeleteAlias($sql);
        $qualifier = $alias ?? $targetTable;

        $selectList = "\"$qualifier\".*";
        if ($columns !== []) {
            $parts = [];
            foreach ($columns as $column) {
                $parts[] = "\"$qualifier\".\"$column\" AS \"$column\"";
            }
            $selectList = implode(', ', $parts);
        }

        $aliasClause = '';
        if ($alias !== null) {
            $aliasClause = ' AS "' . $alias . '"';
        }

        $usingClause = '';
        $using = $this->parser->extractDeleteUsingClause($sql);
        if ($using !== null) {
            $usingClause = ', ' . $using;
        }

        $whereClause = '';
        $where = $this->parser->extractWhereClause($sql);
        if ($where !== null && $where !== '') {
            $whereClause = ' WHERE ' . $where;
        }

        $resultSql = "SELECT $selectList FROM \"$targetTable\"$aliasClause$usingClause$whereClause";

        /** @var array<string, array{alias: string}> $allTables */
        $allTables = [$targetTable => ['alias' => $qualifier]];

        return ['sql' => $resultSql, 'table' => $targetTable, 'tables' => $allTables];
    }
}
