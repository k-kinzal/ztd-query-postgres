<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres;

use ZtdQuery\Exception\UnknownSchemaException;
use ZtdQuery\Exception\UnsupportedSqlException;
use ZtdQuery\Rewrite\MultiRewritePlan;
use ZtdQuery\Rewrite\QueryKind;
use ZtdQuery\Rewrite\AffectedRowsMode;
use ZtdQuery\Rewrite\RewritePlan;
use ZtdQuery\Rewrite\SqlRewriter;
use ZtdQuery\Rewrite\RewriteStateCommitter;
use ZtdQuery\Schema\TableDefinitionRegistry;
use ZtdQuery\Schema\ViewDefinitionSet;
use ZtdQuery\Shadow\ShadowStore;
use ZtdQuery\Sql\TransactionStatement;

/**
 * PostgreSQL rewrite implementation for ZTD.
 *
 * Orchestrates parsing, classification, transformation, and mutation resolution.
 * Uses Result Select Query approach (not RETURNING) for consistency across platforms.
 */
final class PgSqlRewriter implements SqlRewriter, RewriteStateCommitter
{
    public function transactionStatement(string $sql): ?TransactionStatement
    {
        return (new PgSqlTransactionStatementParser())->parse($sql);
    }

    private PgSqlQueryGuard $guard;
    private ShadowStore $shadowStore;
    private TableDefinitionRegistry $registry;
    private PgSqlTransformer $transformer;
    private PgSqlMutationResolver $mutationResolver;
    private PgSqlParser $parser;
    private PgSqlReturningProjectionParser $returningProjectionParser;
    private PgSqlCteShadowComposer $cteComposer;
    private PgSqlPartitionPredicateRenderer $partitionPredicateRenderer;
    private ViewDefinitionSet $views;

    public function __construct(
        PgSqlQueryGuard $guard,
        ShadowStore $shadowStore,
        TableDefinitionRegistry $registry,
        PgSqlTransformer $transformer,
        PgSqlMutationResolver $mutationResolver,
        PgSqlParser $parser,
        ?ViewDefinitionSet $views = null,
    ) {
        $this->guard = $guard;
        $this->shadowStore = $shadowStore;
        $this->registry = $registry;
        $this->transformer = $transformer;
        $this->mutationResolver = $mutationResolver;
        $this->parser = $parser;
        $this->returningProjectionParser = new PgSqlReturningProjectionParser();
        $this->cteComposer = new PgSqlCteShadowComposer();
        $this->partitionPredicateRenderer = new PgSqlPartitionPredicateRenderer();
        $this->views = $views ?? new ViewDefinitionSet();
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

        $statements = $this->splitStatements($sql);
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

        $statements = $this->splitStatements($sql);
        if ($statements === []) {
            throw new UnsupportedSqlException($sql, 'Empty or unparseable');
        }

        $plans = [];
        foreach ($statements as $stmt) {
            $plans[] = $this->rewriteStatement($stmt);
        }

        return new MultiRewritePlan($plans);
    }

    /** {@inheritDoc} */
    public function splitStatements(string $sql): array
    {
        return $this->parser->splitStatements($sql);
    }

    public function commitRewriteState(): void
    {
        $this->transformer->commitRewriteState();
    }

    private function rewriteStatement(string $sql): RewritePlan
    {
        if (PgSqlReadOnlyDiagnosticStatement::isSafe($sql)) {
            return new RewritePlan($sql, QueryKind::READ);
        }
        $kind = $this->guard->classify($sql);
        if ($kind === null) {
            throw new UnsupportedSqlException($sql, 'Statement type not supported');
        }

        if ($kind === QueryKind::SKIPPED) {
            return new RewritePlan($sql, QueryKind::SKIPPED);
        }

        $statementType = $this->parser->classifyStatement($sql);
        if ($statementType === 'DO') {
            return new RewritePlan($sql, QueryKind::READ);
        }
        $tableContext = $this->buildTableContext();

        if ($kind === QueryKind::READ) {
            if ($this->hasSchemaContext()) {
                $tableNames = $this->parser->extractSelectTableNames($sql);
                $declaredCtes = array_fill_keys($this->cteComposer->declaredCteNames($sql), true);
                foreach ($tableNames as $tableName) {
                    if (isset($declaredCtes[strtolower($tableName)])) {
                        continue;
                    }
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

            return new RewritePlan($this->emptyResultSelect(), QueryKind::DDL_SIMULATED, $mutation);
        }

        $mutation = $this->mutationResolver->resolve($sql, $statementType ?? '', $kind);

        if ($statementType === 'TRUNCATE') {
            return new RewritePlan($this->emptyResultSelect(), QueryKind::WRITE_SIMULATED, $mutation);
        }

        $transformedSql = $this->transformer->transform($sql, $tableContext);

        return new RewritePlan(
            $transformedSql,
            QueryKind::WRITE_SIMULATED,
            $mutation,
            $this->returningProjectionParser->parse($sql),
            $statementType === 'MERGE' ? AffectedRowsMode::Changed : AffectedRowsMode::Matched,
        );
    }

    /**
     * Build the table context map for transformers.
     *
     * @return array<string, array{viewSql: string}|array{
     *     rows: array<int, array<string, mixed>>,
     *     columns: array<int, string>,
     *     columnTypes: array<string, \ZtdQuery\Schema\ColumnType>,
     *     columnDefaults: array<string, string>,
     *     identityStrategies: array<string, \ZtdQuery\Schema\IdentityGenerationStrategy>,
     *     generatedExpressions: array<string, string>,
     *     sourceSql?: string,
     *     storageTable?: string
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
            $columnDefaults = $definition !== null ? $definition->columnDefaults : [];
            $identityStrategies = $definition !== null ? $definition->identityStrategies : [];
            $generatedExpressions = $definition !== null ? $definition->generatedExpressions : [];

            $context[$tableName] = [
                'rows' => $rows,
                'columns' => $columns ?? [],
                'columnTypes' => $columnTypes,
                'columnDefaults' => $columnDefaults,
                'identityStrategies' => $identityStrategies,
                'generatedExpressions' => $generatedExpressions,
                'primaryKeys' => $definition !== null ? $definition->primaryKeys : [],
                'candidateKeys' => $definition !== null ? $definition->candidateKeys()->keys() : [],
                'partialUniqueIndexes' => $definition !== null ? $definition->partialUniqueIndexes : [],
            ];
        }

        $allDefinitions = $this->registry->getAll();
        foreach ($allDefinitions as $tableName => $definition) {
            if (isset($context[$tableName])) {
                continue;
            }

            $definitionContext = [
                'rows' => [],
                'columns' => $definition->columns,
                'columnTypes' => $definition->typedColumns,
                'columnDefaults' => $definition->columnDefaults,
                'identityStrategies' => $definition->identityStrategies,
                'generatedExpressions' => $definition->generatedExpressions,
                'primaryKeys' => $definition->primaryKeys,
                'candidateKeys' => $definition->candidateKeys()->keys(),
            ];
            $definitionContext['partialUniqueIndexes'] = $definition->partialUniqueIndexes;
            $context[$tableName] = $definitionContext;
        }

        $quoter = new PgSqlIdentifierQuoter();
        foreach ($allDefinitions as $tableName => $definition) {
            $relation = $definition->partitionRelation;
            if ($relation === null) {
                continue;
            }

            $siblingPredicates = [];
            foreach ($allDefinitions as $siblingDefinition) {
                $sibling = $siblingDefinition->partitionRelation;
                if ($sibling !== null
                    && strcasecmp($sibling->parentTable, $relation->parentTable) === 0
                    && $sibling->predicate !== null
                ) {
                    $siblingPredicates[] = $sibling->predicate;
                }
            }
            $predicate = $this->partitionPredicateRenderer->render($relation, $siblingPredicates);
            $storageTable = $this->storageTable($tableName);
            $partitionContext = $context[$tableName] ?? null;
            if ($partitionContext === null) {
                continue;
            }
            $partitionContext['rows'] = $this->shadowStore->get($storageTable);
            $partitionContext['storageTable'] = $storageTable;
            $partitionContext['sourceSql'] = 'SELECT * FROM '
                . $quoter->quote($relation->parentTable)
                . " WHERE $predicate";
            $context[$tableName] = $partitionContext;
        }

        foreach ((new PgSqlViewShadowRenderer())->render($this->views, array_keys($context)) as $viewName => $viewSql) {
            if (isset($context[$viewName])) {
                continue;
            }
            $context[$viewName] = ['viewSql' => $viewSql];
        }

        return $context;
    }

    private function storageTable(string $tableName): string
    {
        $seen = [];
        while (!in_array($tableName, $seen, true)) {
            $seen[] = $tableName;
            $parent = $this->registry->get($tableName)?->partitionRelation?->parentTable;
            if ($parent === null) {
                return $tableName;
            }
            $tableName = $parent;
        }

        return $tableName;
    }

    private function tableExists(string $tableName): bool
    {
        if ($this->shadowStore->has($tableName)) {
            return true;
        }

        if ($this->registry->has($tableName)) {
            return true;
        }

        if ($this->views->has($tableName)) {
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

        if ($this->views->hasAnyViews()) {
            return true;
        }

        return false;
    }

    public function emptyResultSelect(): string
    {
        return 'SELECT 1 WHERE FALSE';
    }
}
