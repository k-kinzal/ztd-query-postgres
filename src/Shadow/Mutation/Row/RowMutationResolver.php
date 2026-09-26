<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Shadow\Mutation\Row;

use ZtdQuery\Exception\UnknownSchemaException;
use ZtdQuery\Exception\UnsupportedSqlException;
use ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\PgSqlUpsertExpressionParser;
use ZtdQuery\Platform\Postgres\Sql\Merge\PgSqlMergeParser;
use ZtdQuery\Platform\Postgres\Sql\PgSqlParser;
use ZtdQuery\Schema\TableDefinitionRegistry;
use ZtdQuery\Shadow\Mutation\Row\DeleteMutation;
use ZtdQuery\Shadow\Mutation\Row\InsertMutation;
use ZtdQuery\Shadow\Mutation\Row\UpdateMutation;
use ZtdQuery\Shadow\Mutation\ShadowMutation;
use ZtdQuery\Shadow\Mutation\Table\MultiTruncateMutation;
use ZtdQuery\Shadow\Mutation\Table\SynchronizeMutation;
use ZtdQuery\Shadow\Mutation\Table\TruncateMutation;
use ZtdQuery\Shadow\Mutation\UpsertMutation;
use ZtdQuery\Shadow\ShadowStore;
use ZtdQuery\Shadow\ShadowTableState;

/**
 * Dml resolver operations for PostgreSQL mutation.
 *
 * @visibility root
 */
final class RowMutationResolver
{
    private readonly PgSqlParser $parser;
    private readonly TableDefinitionRegistry $registry;
    private readonly ShadowStore $shadowStore;

    /**
     * Supplies the dependencies used by this RowMutationResolver.
     */
    public function __construct(PgSqlParser $parser, TableDefinitionRegistry $registry, ShadowStore $shadowStore)
    {
        $this->parser = $parser;
        $this->registry = $registry;
        $this->shadowStore = $shadowStore;
    }

    /**
     * Resolve insert.
     * @throws UnsupportedSqlException
     */
    public function resolveInsert(string $sql): ShadowMutation
    {
        $tableName = $this->parser->extractInsertTable($sql);
        if ($tableName === null) {
            throw new UnsupportedSqlException($sql, 'Cannot resolve INSERT target');
        }

        $definition = $this->registry->get($tableName);
        $primaryKeys = $definition !== null ? $definition->primaryKeys : [];
        $storageTable = (new \ZtdQuery\Platform\Postgres\Rewrite\Partition\StorageTable($this->registry))->storageTable($tableName);

        if ($this->parser->hasOnConflict($sql)) {
            $conflictInfo = $this->parser->extractOnConflictUpdateColumns($sql);
            $updateColumns = $conflictInfo['columns'];
            $candidateKeys = $definition?->candidateKeys();
            $conflictPredicate = null;
            $target = $this->parser->extractOnConflictTarget($sql);
            if ($target !== null && $definition !== null) {
                $resolved = $target->resolve(
                    $definition->candidateKeys(),
                    $definition->partialUniqueIndexes,
                    $sql,
                );
                $candidateKeys = $resolved['keys'];
                $conflictPredicate = $resolved['predicate'];
            }
            $expressionParser = new PgSqlUpsertExpressionParser();

            if ($updateColumns !== []) {
                return $this->resolveUpsert($sql, $tableName, $storageTable, $primaryKeys, $conflictInfo, $candidateKeys, $conflictPredicate);
            }

            return new InsertMutation(
                $storageTable,
                $primaryKeys,
                true,
                candidateKeys: $candidateKeys,
                conflictPredicate: $conflictPredicate !== null
                    ? $expressionParser->parse($conflictPredicate, $tableName)
                    : null,
            );
        }

        return $this->resolvePlainInsert($sql, $storageTable, $primaryKeys, $definition);
    }

    /**
     * Preserve the reflected constraints when resolving an ordinary INSERT.
     *
     * @param list<string> $primaryKeys
     */
    public function resolvePlainInsert(string $sql, string $storageTable, array $primaryKeys, ?\ZtdQuery\Schema\TableDefinition $definition): ShadowMutation
    {
        return new InsertMutation(
            $storageTable,
            $primaryKeys,
            false,
            tableDefinition: $definition,
            sql: $sql,
            validateConstraints: true,
            candidateKeys: $definition?->candidateKeys(),
        );
    }

    /**
     * Resolve update.
     * @throws UnknownSchemaException
     * @throws UnsupportedSqlException
     */
    public function resolveUpdate(string $sql): ShadowMutation
    {
        $targetTable = $this->parser->extractUpdateTable($sql);
        if ($targetTable === null) {
            throw new UnsupportedSqlException($sql, 'Cannot resolve UPDATE target');
        }

        $definition = $this->registry->get($targetTable);
        if ($definition === null && $this->shadowStore->state($targetTable) !== ShadowTableState::Initialized) {
            throw new UnknownSchemaException($sql, $targetTable, 'table');
        }

        $storageTable = (new \ZtdQuery\Platform\Postgres\Rewrite\Partition\StorageTable($this->registry))->storageTable($targetTable);
        $this->shadowStore->ensure($storageTable);
        $primaryKeys = $definition !== null ? $definition->primaryKeys : [];

        return new UpdateMutation($storageTable, $primaryKeys);
    }

    /**
     * Resolve delete.
     * @throws UnknownSchemaException
     * @throws UnsupportedSqlException
     */
    public function resolveDelete(string $sql): ShadowMutation
    {
        $targetTable = $this->parser->extractDeleteTable($sql);
        if ($targetTable === null) {
            throw new UnsupportedSqlException($sql, 'Cannot resolve DELETE target');
        }

        $definition = $this->registry->get($targetTable);
        if ($definition === null && $this->shadowStore->state($targetTable) !== ShadowTableState::Initialized) {
            throw new UnknownSchemaException($sql, $targetTable, 'table');
        }

        $storageTable = (new \ZtdQuery\Platform\Postgres\Rewrite\Partition\StorageTable($this->registry))->storageTable($targetTable);
        $this->shadowStore->ensure($storageTable);
        $primaryKeys = $definition !== null ? $definition->primaryKeys : [];

        return new DeleteMutation($storageTable, $primaryKeys);
    }

    /**
     * Resolve merge.
     * @throws UnknownSchemaException
     */
    public function resolveMerge(string $sql): ShadowMutation
    {
        $statement = (new PgSqlMergeParser())->parse($sql);
        $definition = $this->registry->get($statement->targetTable);
        if ($definition === null
            && $this->shadowStore->state($statement->targetTable) !== ShadowTableState::Initialized
        ) {
            throw new UnknownSchemaException($sql, $statement->targetTable, 'table');
        }

        return new SynchronizeMutation(
            (new \ZtdQuery\Platform\Postgres\Rewrite\Partition\StorageTable($this->registry))->storageTable($statement->targetTable),
            $definition,
            $sql,
        );
    }

    /**
     * Resolve truncate.
     * @throws UnsupportedSqlException
     */
    public function resolveTruncate(string $sql): ShadowMutation
    {
        $tableNames = $this->parser->extractTruncateTables($sql);
        if ($tableNames === []) {
            throw new UnsupportedSqlException($sql, 'Cannot resolve TRUNCATE target');
        }
        if (count($tableNames) > 1) {
            return new MultiTruncateMutation($tableNames);
        }

        return new TruncateMutation($tableNames[0]);
    }
    /**
     * Resolves update expressions using database evaluation whenever a candidate key permits it.
     * @param list<string> $primaryKeys
     * @param array{columns: list<string>, values: array<string, string>} $conflictInfo
     * @throws UnsupportedSqlException
     */
    public function resolveUpsert(string $sql, string $tableName, string $storageTable, array $primaryKeys, array $conflictInfo, ?\ZtdQuery\Schema\Key\CandidateKeySet $candidateKeys, ?string $conflictPredicate): ShadowMutation
    {
        $expressionParser = new PgSqlUpsertExpressionParser();
        $updateColumns = $conflictInfo['columns'];
        $databaseEvaluated = $candidateKeys !== null && $candidateKeys->keys() !== [];
        /**
         * @var array<string, \ZtdQuery\Shadow\Mutation\UpsertExpression|null> $resolvedValues
         */
        $resolvedValues = [];
        foreach ($conflictInfo['values'] as $col => $value) {
            $resolvedValues[$col] = $databaseEvaluated
                ? $expressionParser->parseIfSupported($value, $tableName)
                : $expressionParser->parse($value, $tableName);
        }
        $predicate = $this->parser->extractOnConflictUpdateWhere($sql);

        return new UpsertMutation(
            $storageTable,
            $primaryKeys,
            $updateColumns,
            $resolvedValues,
            $candidateKeys,
            $predicate !== null
                ? ($databaseEvaluated
                    ? $expressionParser->parseIfSupported($predicate, $tableName)
                    : $expressionParser->parse($predicate, $tableName))
                : null,
            databaseEvaluated: $databaseEvaluated,
            updateSqlValues: $conflictInfo['values'],
            updateSqlPredicate: $predicate,
            conflictPredicate: $conflictPredicate !== null
                ? $expressionParser->parse($conflictPredicate, $tableName)
                : null,
        );
    }
}
