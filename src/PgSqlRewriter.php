<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres;

use ZtdQuery\Exception\UnknownSchemaException;
use ZtdQuery\Exception\UnsupportedSqlException;
use ZtdQuery\Rewrite\MultiRewritePlan;
use ZtdQuery\Rewrite\QueryKind;
use ZtdQuery\Rewrite\RewritePlan;
use ZtdQuery\Rewrite\SqlRewriter;
use ZtdQuery\Schema\TableDefinitionRegistry;
use ZtdQuery\Shadow\ShadowStore;

/**
 * PostgreSQL rewrite implementation for ZTD.
 *
 * Orchestrates parsing, classification, transformation, and mutation resolution.
 * Uses Result Select Query approach (not RETURNING) for consistency across platforms.
 */
final class PgSqlRewriter implements SqlRewriter
{
    private PgSqlQueryGuard $guard;
    private ShadowStore $shadowStore;
    private TableDefinitionRegistry $registry;
    private PgSqlTransformer $transformer;
    private PgSqlMutationResolver $mutationResolver;
    private PgSqlParser $parser;

    public function __construct(
        PgSqlQueryGuard $guard,
        ShadowStore $shadowStore,
        TableDefinitionRegistry $registry,
        PgSqlTransformer $transformer,
        PgSqlMutationResolver $mutationResolver,
        PgSqlParser $parser
    ) {
        $this->guard = $guard;
        $this->shadowStore = $shadowStore;
        $this->registry = $registry;
        $this->transformer = $transformer;
        $this->mutationResolver = $mutationResolver;
        $this->parser = $parser;
    }

    /**
     * {@inheritDoc}
     *
     * @throws UnsupportedSqlException When SQL is empty, unparseable, or multi-statement.
     * @throws UnknownSchemaException When SQL references unknown tables/columns.
     */
    public function rewrite(string $sql): RewritePlan
    {
        $sql = trim($sql);
        if ($sql === '') {
            throw new UnsupportedSqlException($sql, 'Empty or unparseable');
        }

        $statements = $this->parser->splitStatements($sql);
        if ($statements === []) {
            throw new UnsupportedSqlException($sql, 'Empty or unparseable');
        }

        if (count($statements) > 1) {
            throw new UnsupportedSqlException($sql, 'Multi-statement');
        }

        return $this->rewriteStatement($statements[0]);
    }

    /**
     * {@inheritDoc}
     *
     * @throws UnsupportedSqlException When SQL is empty or unparseable.
     * @throws UnknownSchemaException When SQL references unknown tables/columns.
     */
    public function rewriteMultiple(string $sql): MultiRewritePlan
    {
        $sql = trim($sql);
        if ($sql === '') {
            throw new UnsupportedSqlException($sql, 'Empty or unparseable');
        }

        $statements = $this->parser->splitStatements($sql);
        if ($statements === []) {
            throw new UnsupportedSqlException($sql, 'Empty or unparseable');
        }

        $plans = [];
        foreach ($statements as $stmt) {
            $plans[] = $this->rewriteStatement($stmt);
        }

        return new MultiRewritePlan($plans);
    }

    private function rewriteStatement(string $sql): RewritePlan
    {
        $kind = $this->guard->classify($sql);
        if ($kind === null) {
            throw new UnsupportedSqlException($sql, 'Statement type not supported');
        }

        if ($kind === QueryKind::SKIPPED) {
            return new RewritePlan($sql, QueryKind::SKIPPED);
        }

        $tableContext = $this->buildTableContext();
        $statementType = $this->parser->classifyStatement($sql);

        if ($kind === QueryKind::READ) {
            if ($this->hasSchemaContext()) {
                $tableNames = $this->parser->extractSelectTableNames($sql);
                foreach ($tableNames as $tableName) {
                    if (!$this->tableExists($tableName)) {
                        throw new UnknownSchemaException($sql, $tableName, 'table');
                    }
                }
            }

            $transformedSql = $this->transformer->transform($sql, $tableContext);

            return new RewritePlan($transformedSql, QueryKind::READ);
        }

        if ($kind === QueryKind::DDL_SIMULATED) {
            $mutation = $this->mutationResolver->resolve($sql, $statementType ?? '', $kind);

            if ($statementType === 'CREATE_TABLE' && $this->parser->hasCreateTableAsSelect($sql)) {
                $selectSql = $this->parser->extractCreateTableSelectSql($sql);
                if ($selectSql !== null) {
                    $transformedSelectSql = $this->transformer->transform($selectSql, $tableContext);

                    return new RewritePlan($transformedSelectSql, QueryKind::DDL_SIMULATED, $mutation);
                }
            }

            return new RewritePlan('SELECT 1 WHERE FALSE', QueryKind::DDL_SIMULATED, $mutation);
        }

        if ($statementType === 'UPDATE' || $statementType === 'DELETE') {
            $this->ensureDmlTarget($sql, $statementType);
        }

        $mutation = $this->mutationResolver->resolve($sql, $statementType ?? '', $kind);

        if ($statementType === 'TRUNCATE') {
            return new RewritePlan('SELECT 1 WHERE FALSE', QueryKind::WRITE_SIMULATED, $mutation);
        }

        $transformedSql = $this->transformer->transform($sql, $tableContext);

        return new RewritePlan($transformedSql, QueryKind::WRITE_SIMULATED, $mutation);
    }

    /**
     * Build the table context map for transformers.
     *
     * @return array<string, array{
     *     rows: array<int, array<string, mixed>>,
     *     columns: array<int, string>,
     *     columnTypes: array<string, \ZtdQuery\Schema\ColumnType>
     * }>
     */
    private function buildTableContext(): array
    {
        $context = [];
        $allData = $this->shadowStore->getAll();

        foreach ($allData as $tableName => $rows) {
            $definition = $this->registry->get($tableName);
            $columns = $definition?->columns;
            if ($columns === null && $rows !== []) {
                $columns = array_keys($rows[0]);
                foreach ($rows as $row) {
                    foreach (array_keys($row) as $column) {
                        if (!in_array($column, $columns, true)) {
                            $columns[] = $column;
                        }
                    }
                }
            }

            $columnTypes = $definition !== null ? $definition->typedColumns : [];

            $context[$tableName] = [
                'rows' => $rows,
                'columns' => $columns ?? [],
                'columnTypes' => $columnTypes,
            ];
        }

        $allDefinitions = $this->registry->getAll();
        foreach ($allDefinitions as $tableName => $definition) {
            if (isset($context[$tableName])) {
                continue;
            }

            $context[$tableName] = [
                'rows' => [],
                'columns' => $definition->columns,
                'columnTypes' => $definition->typedColumns,
            ];
        }

        return $context;
    }

    private function ensureDmlTarget(string $sql, string $statementType): void
    {
        if ($statementType === 'UPDATE') {
            $targetTable = $this->parser->extractUpdateTable($sql);
            if ($targetTable !== null) {
                $this->shadowStore->ensure($targetTable);
            }
        }

        if ($statementType === 'DELETE') {
            $targetTable = $this->parser->extractDeleteTable($sql);
            if ($targetTable !== null) {
                $this->shadowStore->ensure($targetTable);
            }
        }
    }

    private function tableExists(string $tableName): bool
    {
        if ($this->shadowStore->get($tableName) !== []) {
            return true;
        }

        if ($this->registry->has($tableName)) {
            return true;
        }

        return false;
    }

    private function hasSchemaContext(): bool
    {
        if ($this->shadowStore->getAll() !== []) {
            return true;
        }

        if ($this->registry->hasAnyTables()) {
            return true;
        }

        return false;
    }
}
