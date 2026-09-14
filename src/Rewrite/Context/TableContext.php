<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Rewrite\Context;

use ZtdQuery\Platform\Postgres\Rewrite\Partition\PgSqlPartitionPredicateRenderer;
use ZtdQuery\Platform\Postgres\Rewrite\View\PgSqlViewShadowRenderer;
use ZtdQuery\Platform\Postgres\Sql\PgSqlIdentifierQuoter;
use ZtdQuery\Schema\TableDefinitionRegistry;
use ZtdQuery\Schema\ViewDefinitionSet;
use ZtdQuery\Shadow\ShadowStore;

/**
 * Schema context operations for PostgreSQL session.
 *
 * @visibility root
 */
final class TableContext
{
    private readonly PgSqlPartitionPredicateRenderer $partitionPredicateRenderer;
    private readonly TableDefinitionRegistry $registry;
    private readonly ShadowStore $shadowStore;
    private readonly ViewDefinitionSet $views;

    /**
     * Supplies the dependencies used by this TableContext.
     */
    public function __construct(PgSqlPartitionPredicateRenderer $partitionPredicateRenderer, TableDefinitionRegistry $registry, ShadowStore $shadowStore, ViewDefinitionSet $views)
    {
        $this->partitionPredicateRenderer = $partitionPredicateRenderer;
        $this->registry = $registry;
        $this->shadowStore = $shadowStore;
        $this->views = $views;
    }

    /**
     * Build the table context map for transformers.
     * @template T
     * @param array<string, array<int, array<string, T>>> $allData
     *
     * @return array<string, array{viewSql: string}|array{
     *     rows: array<int, array<string, T>>,
     *     columns: array<int, string>,
     *     columnTypes: array<string, \ZtdQuery\Schema\ColumnDeclaration>,
     *     columnDefaults: array<string, string>,
     *     identityStrategies: array<string, \ZtdQuery\Schema\Key\IdentityGenerationStrategy>,
     *     generatedExpressions: array<string, string>,
     *     sourceSql?: string,
     *     storageTable?: string
     * }>
     */
    public function buildTableContext(array $allData): array
    {
        $context = $this->dataContext($allData);
        $context = $this->fillDefinitions($context);
        $context = $this->applyPartitions($context, $allData);

        foreach ((new PgSqlViewShadowRenderer())->render($this->views, array_keys($context)) as $viewName => $viewSql) {
            if (isset($context[$viewName])) {
                continue;
            }
            $context[$viewName] = ['viewSql' => $viewSql];
        }

        return $context;
    }

    /**
     * Table exists.
     */
    public function tableExists(string $tableName): bool
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

    /**
     * Has schema context.
     */
    public function hasSchemaContext(): bool
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
    /**
     * Derives declaration metadata and columns for materialized shadow rows.
     * @template T
     * @param array<string, array<int, array<string, T>>> $allData
     * @return array<string, array{rows: array<int, array<string, T>>, columns: array<int, string>, columnTypes: array<string, \ZtdQuery\Schema\ColumnDeclaration>, columnDefaults: array<string, string>, identityStrategies: array<string, \ZtdQuery\Schema\Key\IdentityGenerationStrategy>, generatedExpressions: array<string, string>, primaryKeys: array<int, string>, candidateKeys: array<string, array<int, string>>, partialUniqueIndexes: array<string, \ZtdQuery\Schema\Key\PartialUniqueIndex>, sourceSql?: string, storageTable?: string}>
     */
    public function dataContext(array $allData): array
    {
        $context = [];

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

        return $context;
    }

    /**
     * Adds reflected tables that have no materialized shadow rows yet.
     * @template T
     * @param array<string, array{rows: array<int, array<string, T>>, columns: array<int, string>, columnTypes: array<string, \ZtdQuery\Schema\ColumnDeclaration>, columnDefaults: array<string, string>, identityStrategies: array<string, \ZtdQuery\Schema\Key\IdentityGenerationStrategy>, generatedExpressions: array<string, string>, primaryKeys: array<int, string>, candidateKeys: array<string, array<int, string>>, partialUniqueIndexes: array<string, \ZtdQuery\Schema\Key\PartialUniqueIndex>, sourceSql?: string, storageTable?: string}> $context
     * @return array<string, array{rows: array<int, array<string, T>>, columns: array<int, string>, columnTypes: array<string, \ZtdQuery\Schema\ColumnDeclaration>, columnDefaults: array<string, string>, identityStrategies: array<string, \ZtdQuery\Schema\Key\IdentityGenerationStrategy>, generatedExpressions: array<string, string>, primaryKeys: array<int, string>, candidateKeys: array<string, array<int, string>>, partialUniqueIndexes: array<string, \ZtdQuery\Schema\Key\PartialUniqueIndex>, sourceSql?: string, storageTable?: string}>
     */
    public function fillDefinitions(array $context): array
    {
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

        return $context;
    }

    /**
     * Projects child partitions from their shared storage table and sibling predicates.
     * @template T
     * @param array<string, array{rows: array<int, array<string, T>>, columns: array<int, string>, columnTypes: array<string, \ZtdQuery\Schema\ColumnDeclaration>, columnDefaults: array<string, string>, identityStrategies: array<string, \ZtdQuery\Schema\Key\IdentityGenerationStrategy>, generatedExpressions: array<string, string>, primaryKeys: array<int, string>, candidateKeys: array<string, array<int, string>>, partialUniqueIndexes: array<string, \ZtdQuery\Schema\Key\PartialUniqueIndex>, sourceSql?: string, storageTable?: string}> $context
     * @param array<string, array<int, array<string, T>>> $allData
     * @return array<string, array{rows: array<int, array<string, T>>, columns: array<int, string>, columnTypes: array<string, \ZtdQuery\Schema\ColumnDeclaration>, columnDefaults: array<string, string>, identityStrategies: array<string, \ZtdQuery\Schema\Key\IdentityGenerationStrategy>, generatedExpressions: array<string, string>, primaryKeys: array<int, string>, candidateKeys: array<string, array<int, string>>, partialUniqueIndexes: array<string, \ZtdQuery\Schema\Key\PartialUniqueIndex>, sourceSql?: string, storageTable?: string}>
     */
    public function applyPartitions(array $context, array $allData): array
    {
        $allDefinitions = $this->registry->getAll();
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
            $storageTable = (new \ZtdQuery\Platform\Postgres\Rewrite\Partition\StorageTable($this->registry))->storageTable($tableName);
            $partitionContext = $context[$tableName] ?? null;
            if ($partitionContext === null) {
                continue;
            }
            $partitionContext['rows'] = $allData[$storageTable] ?? [];
            $partitionContext['storageTable'] = $storageTable;
            $partitionContext['sourceSql'] = 'SELECT * FROM '
                . $quoter->quote($relation->parentTable)
                . " WHERE $predicate";
            $context[$tableName] = $partitionContext;
        }

        return $context;
    }
}
