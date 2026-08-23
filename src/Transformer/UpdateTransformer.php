<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Transformer;

use ZtdQuery\Exception\UnsupportedSqlException;
use ZtdQuery\Platform\Postgres\PgSqlCteShadowComposer;
use ZtdQuery\Platform\Postgres\PgSqlParser;
use ZtdQuery\Rewrite\SqlTransformer;
use ZtdQuery\Shadow\Mutation\MutationRowIdentity;

/**
 * Transforms UPDATE statements into SELECT projections with CTE shadowing.
 *
 * PostgreSQL differences:
 * - FROM clause for multi-table updates (not comma-separated table list)
 * - Double-quote identifiers
 */
final class UpdateTransformer implements SqlTransformer
{
    private PgSqlParser $parser;
    private SelectTransformer $selectTransformer;
    private PgSqlCteShadowComposer $cteComposer;

    public function __construct(
        PgSqlParser $parser,
        SelectTransformer $selectTransformer,
    ) {
        $this->parser = $parser;
        $this->selectTransformer = $selectTransformer;
        $this->cteComposer = new PgSqlCteShadowComposer();
    }

    /**
     * {@inheritDoc}
     */
    public function transform(string $sql, array $tables): string
    {
        $targetTable = $this->parser->extractUpdateTable($sql);
        if ($targetTable === null) {
            throw new UnsupportedSqlException($sql, 'Cannot resolve UPDATE target');
        }

        $columns = $tables[$targetTable]['columns'] ?? [];
        $projection = $this->buildProjection(
            $sql,
            $targetTable,
            $columns,
            $tables[$targetTable]['primaryKeys'] ?? [],
        );

        return $this->selectTransformer->transform(
            $this->cteComposer->carryPrefix($sql, $projection['sql']),
            $tables,
        );
    }

    /**
     * Build a result-select SQL from an UPDATE statement.
     *
     * @param array<int, string> $columns
     * @param array<int, string> $primaryKeys
     * @return array{sql: string, table: string, tables: array<string, array{alias: string}>}
     */
    public function buildProjection(
        string $sql,
        string $targetTable,
        array $columns,
        array $primaryKeys = [],
    ): array {
        $alias = $this->parser->extractUpdateAlias($sql);
        $qualifier = $alias ?? $targetTable;

        $sets = $this->parser->extractUpdateSets($sql);

        $selectCols = [];
        $coveredCols = [];

        foreach ($sets as $colName => $value) {
            $selectCols[] = $value . ' AS "' . $colName . '"';
            $coveredCols[$colName] = true;
        }

        foreach ($columns as $col) {
            if (!isset($coveredCols[$col])) {
                $selectCols[] = "\"$qualifier\".\"$col\"";
            }
        }

        $identity = new MutationRowIdentity();
        foreach ($primaryKeys as $primaryKey) {
            $selectCols[] = '"' . $qualifier . '"."' . $primaryKey . '" AS "' . $identity->column($primaryKey) . '"';
        }

        if ($selectCols === []) {
            $selectCols[] = '*';
        }

        $selectList = implode(', ', $selectCols);

        $aliasClause = '';
        if ($alias !== null) {
            $aliasClause = ' AS "' . $alias . '"';
        }

        $fromClause = $this->parser->extractUpdateFromClause($sql);
        $additionalFrom = '';
        if ($fromClause !== null) {
            $additionalFrom = ', ' . $fromClause;
        }

        $whereClause = '';
        $where = $this->parser->extractWhereClause($sql);
        if ($where !== null && $where !== '') {
            $whereClause = ' WHERE ' . $where;
        }

        $resultSql = "SELECT $selectList FROM \"$targetTable\"$aliasClause$additionalFrom$whereClause";

        /** @var array<string, array{alias: string}> $allTables */
        $allTables = [$targetTable => ['alias' => $qualifier]];

        return ['sql' => $resultSql, 'table' => $targetTable, 'tables' => $allTables];
    }
}
