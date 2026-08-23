<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Transformer;

use ZtdQuery\Exception\UnsupportedSqlException;
use ZtdQuery\Platform\Postgres\PgSqlIdentifierQuoter;
use ZtdQuery\Platform\Postgres\PgSqlCteShadowComposer;
use ZtdQuery\Platform\Postgres\PgSqlGeneratedColumnProjector;
use ZtdQuery\Platform\Postgres\PgSqlMergeActionKind;
use ZtdQuery\Platform\Postgres\PgSqlMergeClause;
use ZtdQuery\Platform\Postgres\PgSqlMergeMatchKind;
use ZtdQuery\Platform\Postgres\PgSqlMergeParser;
use ZtdQuery\Platform\Postgres\PgSqlMergeStatement;
use ZtdQuery\Rewrite\ShadowIdentityAllocator;
use ZtdQuery\Rewrite\SqlTransformer;

final class MergeTransformer implements SqlTransformer
{
    private PgSqlIdentifierQuoter $quoter;
    private InsertRowRenderer $rowRenderer;
    private PgSqlGeneratedColumnProjector $generatedColumnProjector;
    private PgSqlCteShadowComposer $cteComposer;
    private InsertSelectRenderer $insertSelectRenderer;

    public function __construct(
        private readonly PgSqlMergeParser $parser,
        private readonly SelectTransformer $selectTransformer,
    ) {
        $this->quoter = new PgSqlIdentifierQuoter();
        $this->rowRenderer = new InsertRowRenderer();
        $this->generatedColumnProjector = new PgSqlGeneratedColumnProjector();
        $this->cteComposer = new PgSqlCteShadowComposer();
        $this->insertSelectRenderer = new InsertSelectRenderer();
    }

    /**
     * {@inheritDoc}
     */
    public function transform(string $sql, array $tables): string
    {
        $statement = $this->parser->parse($sql);
        $table = $tables[$statement->targetTable] ?? null;
        if ($table === null) {
            throw new UnsupportedSqlException($sql, 'Cannot resolve MERGE target schema');
        }
        if (isset($table['viewSql'])) {
            throw new UnsupportedSqlException($sql, 'Cannot resolve MERGE target schema');
        }

        $columns = $table['columns'];
        if (!array_is_list($columns)) {
            throw new UnsupportedSqlException($sql, 'MERGE target columns must preserve declaration order');
        }
        if ($columns === []) {
            throw new UnsupportedSqlException($sql, 'Cannot determine MERGE target columns');
        }
        if (($table['storageTable'] ?? $statement->targetTable) !== $statement->targetTable) {
            throw new UnsupportedSqlException($sql, 'MERGE into a child partition is not supported');
        }

        $defaults = $table['columnDefaults'] ?? [];
        $identityStrategies = $table['identityStrategies'] ?? [];
        $existingRows = $table['rows'];
        $generatedExpressions = $table['generatedExpressions'] ?? [];
        $effectiveConditions = $this->effectiveConditions($statement);
        $parts = [];
        $parts[] = $this->unchangedRows($statement, $columns, $effectiveConditions);

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

        $resultSql = implode(' UNION ALL ', $parts);
        $resultSql = $this->generatedColumnProjector->project($resultSql, $columns, $generatedExpressions);
        $resultSql = $this->cteComposer->carryPrefix($sql, $resultSql);

        return $this->selectTransformer->transform($resultSql, $tables);
    }

    /**
     * @return list<string>
     */
    private function effectiveConditions(PgSqlMergeStatement $statement): array
    {
        $priorMatched = [];
        $priorNotMatched = [];
        $effective = [];
        foreach ($statement->clauses as $clause) {
            $prior = $clause->matchKind === PgSqlMergeMatchKind::Matched
                ? $priorMatched
                : $priorNotMatched;
            $condition = $clause->conditionSql ?? 'TRUE';
            $predicate = '(' . $condition . ')';
            if ($prior !== []) {
                $excluded = array_map(
                    static fn (string $previous): string => 'COALESCE((' . $previous . '), FALSE)',
                    $prior,
                );
                $predicate .= ' AND NOT (' . implode(' OR ', $excluded) . ')';
            }
            $effective[] = $predicate;
            if ($clause->matchKind === PgSqlMergeMatchKind::Matched) {
                $priorMatched[] = $condition;
            } else {
                $priorNotMatched[] = $condition;
            }
        }

        return $effective;
    }

    /**
     * @param list<string> $columns
     * @param list<string> $effectiveConditions
     */
    private function unchangedRows(
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
     */
    private function updatedRows(
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
     * @param list<string> $columns
     * @param array<string, string> $defaults
     * @param array<string, \ZtdQuery\Schema\IdentityGenerationStrategy> $identityStrategies
     * @param array<int, array<string, mixed>> $existingRows
     */
    private function insertedRows(
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

        try {
            $providedExpressions = $this->rowRenderer->providedExpressions($sourceColumns, $clause->insertValues);
        } catch (\InvalidArgumentException) {
            throw new UnsupportedSqlException($sql, 'MERGE INSERT values count does not match column count');
        }
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

}
