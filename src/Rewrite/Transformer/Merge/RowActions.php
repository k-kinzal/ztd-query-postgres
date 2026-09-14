<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Rewrite\Transformer\Merge;

use ZtdQuery\Exception\UnsupportedSqlException;
use ZtdQuery\Platform\Postgres\Rewrite\Transformer\InsertRowRenderer;
use ZtdQuery\Platform\Postgres\Rewrite\Transformer\InsertSelectRenderer;
use ZtdQuery\Platform\Postgres\Sql\Merge\PgSqlMergeActionKind;
use ZtdQuery\Platform\Postgres\Sql\Merge\PgSqlMergeClause;
use ZtdQuery\Platform\Postgres\Sql\Merge\PgSqlMergeMatchKind;
use ZtdQuery\Platform\Postgres\Sql\Merge\PgSqlMergeStatement;
use ZtdQuery\Platform\Postgres\Sql\PgSqlIdentifierQuoter;
use ZtdQuery\Rewrite\ShadowIdentityAllocator;

/**
 * Row actions operations for PostgreSQL merge.
 *
 * @visibility root
 */
final class RowActions
{
    private readonly InsertSelectRenderer $insertSelectRenderer;
    private readonly PgSqlIdentifierQuoter $quoter;
    private readonly InsertRowRenderer $rowRenderer;

    /**
     * Supplies the dependencies used by this RowActions.
     */
    public function __construct(InsertSelectRenderer $insertSelectRenderer, PgSqlIdentifierQuoter $quoter, InsertRowRenderer $rowRenderer)
    {
        $this->insertSelectRenderer = $insertSelectRenderer;
        $this->quoter = $quoter;
        $this->rowRenderer = $rowRenderer;
    }

    /**
     * @param list<string> $columns
     * @param list<string> $effectiveConditions
     */
    public function unchangedRows(
        PgSqlMergeStatement $statement,
        array $columns,
        array $effectiveConditions,
    ): string {
        $qualifier = $this->quoter->quote($statement->targetAlias);
        $selects = [];
        foreach ($columns as $column) {
            $quoted = $this->quoter->quote($column);
            $selects[] = $qualifier . '.' . $quoted . ' AS ' . $quoted;
        }

        $modifications = [];
        foreach ($statement->clauses as $index => $clause) {
            if ($clause->matchKind !== PgSqlMergeMatchKind::Matched
                || !in_array($clause->actionKind, [PgSqlMergeActionKind::Update, PgSqlMergeActionKind::Delete], true)
            ) {
                continue;
            }
            $modifications[] = 'EXISTS (SELECT 1 FROM ' . $statement->sourceSql
                . ' WHERE (' . $statement->joinConditionSql . ') AND (' . $effectiveConditions[$index] . '))';
        }

        $sql = 'SELECT ' . implode(', ', $selects)
            . ' FROM ' . $statement->targetSql . ' AS ' . $qualifier;
        if ($modifications !== []) {
            $sql .= ' WHERE NOT (' . implode(' OR ', $modifications) . ')';
        }

        return $sql;
    }

    /**
     * @param list<string> $columns
     * @param array<string, string> $defaults
     * @throws UnsupportedSqlException
     */
    public function updatedRows(
        string $sql,
        PgSqlMergeStatement $statement,
        PgSqlMergeClause $clause,
        array $columns,
        array $defaults,
        string $effectiveCondition,
    ): string {
        foreach (array_keys($clause->assignments) as $column) {
            if (!in_array($column, $columns, true)) {
                throw new UnsupportedSqlException($sql, 'MERGE UPDATE references an unknown target column');
            }
        }

        $qualifier = $this->quoter->quote($statement->targetAlias);
        $selects = [];
        foreach ($columns as $column) {
            $quoted = $this->quoter->quote($column);
            $expression = $clause->assignments[$column] ?? null;
            if ($expression === null) {
                $expression = $qualifier . '.' . $quoted;
            } elseif (strcasecmp($expression, 'DEFAULT') === 0) {
                $expression = $defaults[$column] ?? 'NULL';
            }
            $selects[] = $expression . ' AS ' . $quoted;
        }

        return 'SELECT ' . implode(', ', $selects)
            . ' FROM ' . $statement->targetSql . ' AS ' . $qualifier
            . ' JOIN ' . $statement->sourceSql
            . ' ON (' . $statement->joinConditionSql . ')'
            . ' WHERE ' . $effectiveCondition;
    }

    /**
     * @template T
     * @param list<string> $columns
     * @param array<string, string> $defaults
     * @param array<string, \ZtdQuery\Schema\Key\IdentityGenerationStrategy> $identityStrategies
     * @param array<int, array<string, T>> $existingRows
     * @throws UnsupportedSqlException
     */
    public function insertedRows(
        string $sql,
        PgSqlMergeStatement $statement,
        PgSqlMergeClause $clause,
        array $columns,
        array $defaults,
        array $identityStrategies,
        array $existingRows,
        string $effectiveCondition,
    ): string {
        if ($clause->insertColumns !== []) {
            $sourceColumns = $clause->insertColumns;
        } elseif ($clause->insertValues === []) {
            $sourceColumns = [];
        } else {
            $sourceColumns = $columns;
        }
        foreach ($sourceColumns as $column) {
            if (!in_array($column, $columns, true)) {
                throw new UnsupportedSqlException($sql, 'MERGE INSERT references an unknown target column');
            }
        }

        if (count($sourceColumns) !== count($clause->insertValues)) {
            throw new UnsupportedSqlException($sql, 'MERGE INSERT values count does not match column count');
        }
        $providedExpressions = $this->rowRenderer->providedExpressions($sourceColumns, $clause->insertValues);
        $generatedStarts = (new ShadowIdentityAllocator())->allocateSelectStarts(
            $statement->targetTable,
            $identityStrategies,
            array_keys($providedExpressions),
            $existingRows,
        );
        foreach ($generatedStarts as $column => $start) {
            $providedExpressions[$column] = $this->insertSelectRenderer->renderGeneratedIdentity($start);
        }
        $projected = $this->rowRenderer->render($columns, $providedExpressions, $defaults);

        $selects = [];
        foreach ($projected as $column => $expression) {
            $selects[] = $expression . ' AS ' . $this->quoter->quote($column);
        }
        $targetAlias = $this->quoter->quote($statement->targetAlias);

        return 'SELECT ' . implode(', ', $selects)
            . ' FROM ' . $statement->sourceSql
            . ' WHERE NOT EXISTS (SELECT 1 FROM ' . $statement->targetSql . ' AS ' . $targetAlias
            . ' WHERE ' . $statement->joinConditionSql . ')'
            . ' AND (' . $effectiveCondition . ')';
    }
    /**
     * Projects UPDATE and INSERT branches using their ordered effective conditions.
     * @template T
     * @param list<string> $columns
     * @param array<string, string> $defaults
     * @param array<string, \ZtdQuery\Schema\Key\IdentityGenerationStrategy> $identityStrategies
     * @param array<int, array<string, T>> $existingRows
     * @param array<int, string> $effectiveConditions
     * @return list<string>
     */
    public function modifiedRows(string $sql, PgSqlMergeStatement $statement, array $columns, array $defaults, array $identityStrategies, array $existingRows, array $effectiveConditions): array
    {
        $parts = [];
        foreach ($statement->clauses as $index => $clause) {
            $effective = $effectiveConditions[$index];
            if ($clause->actionKind === PgSqlMergeActionKind::Update) {
                $parts[] = $this->updatedRows($sql, $statement, $clause, $columns, $defaults, $effective);
            }
            if ($clause->actionKind === PgSqlMergeActionKind::Insert) {
                $parts[] = $this->insertedRows(
                    $sql,
                    $statement,
                    $clause,
                    $columns,
                    $defaults,
                    $identityStrategies,
                    $existingRows,
                    $effective,
                );
            }
        }

        return $parts;
    }
}
