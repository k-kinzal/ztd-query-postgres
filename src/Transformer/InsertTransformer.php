<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Transformer;

use ZtdQuery\Exception\UnsupportedSqlException;
use ZtdQuery\Platform\CastRenderer;
use ZtdQuery\Platform\Postgres\PgSqlCastRenderer;
use ZtdQuery\Platform\Postgres\PgSqlNativeUpsertProjector;
use ZtdQuery\Platform\Postgres\PgSqlLexerProfile;
use ZtdQuery\Platform\Postgres\PgSqlParser;
use ZtdQuery\Platform\Postgres\PgSqlCteShadowComposer;
use ZtdQuery\Rewrite\ShadowIdentityAllocator;
use ZtdQuery\Rewrite\SqlTransformer;
use ZtdQuery\Schema\CandidateKeySet;
use ZtdQuery\Schema\ColumnType;
use ZtdQuery\Schema\ColumnTypeFamily;
use ZtdQuery\Schema\PartialUniqueIndex;
use ZtdQuery\Sql\SqlTokenStream;

/**
 * Transforms INSERT statements into SELECT queries that return the inserted rows.
 * Applies CTE shadowing via the SelectTransformer delegate.
 */
final class InsertTransformer implements SqlTransformer
{
    private PgSqlParser $parser;
    private SelectTransformer $selectTransformer;
    private CastRenderer $castRenderer;
    private InsertRowRenderer $rowRenderer;
    private ShadowIdentityAllocator $identityAllocator;
    private InsertSelectRenderer $insertSelectRenderer;
    private PgSqlCteShadowComposer $cteComposer;
    private PgSqlNativeUpsertProjector $upsertProjector;

    public function __construct(
        PgSqlParser $parser,
        SelectTransformer $selectTransformer,
        ?CastRenderer $castRenderer = null,
    ) {
        $this->parser = $parser;
        $this->selectTransformer = $selectTransformer;
        $this->castRenderer = $castRenderer ?? new PgSqlCastRenderer();
        $this->rowRenderer = new InsertRowRenderer();
        $this->identityAllocator = new ShadowIdentityAllocator();
        $this->insertSelectRenderer = new InsertSelectRenderer();
        $this->cteComposer = new PgSqlCteShadowComposer();
        $this->upsertProjector = new PgSqlNativeUpsertProjector();
    }

    /**
     * {@inheritDoc}
     */
    public function transform(string $sql, array $tables): string
    {
        $this->identityAllocator->beginProjection();
        $tableName = $this->parser->extractInsertTable($sql);
        if ($tableName === null) {
            throw new UnsupportedSqlException($sql, 'Cannot resolve INSERT target');
        }

        $insertColumns = $this->parser->extractInsertColumns($sql);
        $tableColumns = self::orderedValues($tables[$tableName]['columns'] ?? $insertColumns);
        if ($tableColumns === []) {
            throw new UnsupportedSqlException($sql, 'Cannot determine columns');
        }

        $columnDefaults = $tables[$tableName]['columnDefaults'] ?? [];
        $identityStrategies = $tables[$tableName]['identityStrategies'] ?? [];
        $existingRows = $tables[$tableName]['rows'] ?? [];
        $identityTable = $tables[$tableName]['storageTable'] ?? $tableName;

        if ($this->parser->hasInsertSelect($sql)) {
            $selectSql = $this->parser->extractInsertSelectSql($sql);
            if ($selectSql === null) {
                throw new UnsupportedSqlException($sql, 'Cannot extract INSERT ... SELECT subquery');
            }

            $sourceColumns = $insertColumns !== [] ? $insertColumns : $tableColumns;
            $generatedIdentityStarts = $this->identityAllocator->allocateSelectStarts(
                $identityTable,
                $identityStrategies,
                $sourceColumns,
                $existingRows,
            );
            $projectedSql = $this->insertSelectRenderer->render(
                $this->cteComposer->carryPrefix($sql, $selectSql),
                $tableColumns,
                $sourceColumns,
                $columnDefaults,
                $generatedIdentityStarts,
            );
            $projectedSql = $this->projectUpsert($sql, $projectedSql, $tableName, $tableColumns, $tables);

            return $this->selectTransformer->transform($projectedSql, $tables);
        }

        $valueRows = SqlTokenStream::tokenize($sql, PgSqlLexerProfile::create())->topLevelClause(['DEFAULT', 'VALUES']) !== null
            ? [[]]
            : $this->parser->extractInsertValues($sql);
        if ($valueRows === []) {
            throw new UnsupportedSqlException($sql, 'Cannot extract INSERT values');
        }

        $selectParts = [];
        $columnTypes = $tables[$tableName]['columnTypes'] ?? [];
        foreach ($valueRows as $values) {
            $sourceColumns = $insertColumns !== [] || $values === [] ? $insertColumns : $tableColumns;
            try {
                $providedExpressions = $this->rowRenderer->providedExpressions($sourceColumns, $values);
            } catch (\InvalidArgumentException) {
                throw new UnsupportedSqlException($sql, 'Insert values count does not match column count');
            }
            $generatedValues = $this->identityAllocator->allocateMissing(
                $identityTable,
                $identityStrategies,
                array_keys($providedExpressions),
                $existingRows,
            );
            $projected = $this->rowRenderer->render($tableColumns, $providedExpressions, $columnDefaults, $generatedValues);

            $selects = [];
            foreach ($projected as $column => $expr) {
                $type = $columnTypes[$column] ?? null;
                if ($type instanceof ColumnType) {
                    $expr = $this->castInsertExpression($expr, $type);
                }
                $selects[] = $expr . ' AS "' . $column . '"';
            }
            $selectParts[] = 'SELECT ' . implode(', ', $selects);
        }

        $selectSql = implode(' UNION ALL ', $selectParts);
        $selectSql = $this->projectUpsert($sql, $selectSql, $tableName, $tableColumns, $tables);

        return $this->selectTransformer->transform(
            $this->cteComposer->carryPrefix($sql, $selectSql),
            $tables,
        );
    }

    /**
     * @param list<string> $tableColumns
     * @param array<string, array{
     *     candidateKeys?: array<string, array<int, string>>,
     *     partialUniqueIndexes?: array<string, PartialUniqueIndex>
     * }> $tables
     */
    private function projectUpsert(
        string $sql,
        string $selectSql,
        string $tableName,
        array $tableColumns,
        array $tables,
    ): string {
        $conflict = $this->parser->extractOnConflictUpdateColumns($sql);
        $candidateKeys = new CandidateKeySet(
            isset($tables[$tableName]['candidateKeys']) ? $tables[$tableName]['candidateKeys'] : [],
        );
        $conflictPredicate = null;
        $target = $this->parser->extractOnConflictTarget($sql);
        if ($target !== null) {
            $resolved = $target->resolve(
                $candidateKeys,
                isset($tables[$tableName]['partialUniqueIndexes'])
                    ? $tables[$tableName]['partialUniqueIndexes']
                    : [],
                $sql,
            );
            $candidateKeys = $resolved['keys'];
            $conflictPredicate = $resolved['predicate'];
        }

        return $this->upsertProjector->project(
            $selectSql,
            $tableName,
            $tableColumns,
            $candidateKeys->keys(),
            $conflict['values'],
            $this->parser->extractOnConflictUpdateWhere($sql),
            $conflictPredicate,
        );
    }

    public function commitRewriteState(): void
    {
        $this->identityAllocator->commitProjection();
    }

    /**
     * @template T
     * @param array<array-key, T> $values
     * @return list<T>
     */
    private static function orderedValues(array $values): array
    {
        $ordered = [];
        foreach ($values as $value) {
            $ordered[] = $value;
        }

        return $ordered;
    }

    private function castInsertExpression(string $expression, ColumnType $type): string
    {
        if ($type->family === ColumnTypeFamily::BOOLEAN && $expression === '?') {
            return "CAST(COALESCE(NULLIF(CAST(? AS TEXT), ''), 'false') AS BOOLEAN)";
        }

        return $this->castRenderer->renderCast($expression, $type);
    }
}
