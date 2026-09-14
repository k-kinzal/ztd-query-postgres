<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Rewrite\Transformer;

use ZtdQuery\Exception\UnsupportedSqlException;
use ZtdQuery\Platform\CastRenderer;
use ZtdQuery\Platform\Postgres\Rewrite\Cte\PgSqlCteShadowComposer;
use ZtdQuery\Platform\Postgres\Rewrite\Upsert\PgSqlNativeUpsertProjector;
use ZtdQuery\Platform\Postgres\Sql\PgSqlParser;
use ZtdQuery\Platform\Postgres\Sql\Value\PgSqlCastRenderer;
use ZtdQuery\Rewrite\ShadowIdentityAllocator;
use ZtdQuery\Rewrite\SqlTransformer;

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

    /**
     * Initializes the collaborators and state used by this insert transformer.
     */
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
     * @throws UnsupportedSqlException
     */
    public function transform(string $sql, array $tables): string
    {
        $this->identityAllocator->beginProjection();
        $tableName = $this->parser->extractInsertTable($sql);
        if ($tableName === null) {
            throw new UnsupportedSqlException($sql, 'Cannot resolve INSERT target');
        }

        $insertColumns = $this->parser->extractInsertColumns($sql);
        $tableColumns = Insert\OrderedExpressions::orderedValues($tables[$tableName]['columns'] ?? $insertColumns);
        if ($tableColumns === []) {
            throw new UnsupportedSqlException($sql, 'Cannot determine columns');
        }

        $columnDefaults = $tables[$tableName]['columnDefaults'] ?? [];
        $identityStrategies = $tables[$tableName]['identityStrategies'] ?? [];
        $existingRows = $tables[$tableName]['rows'] ?? [];
        $identityTable = $tables[$tableName]['storageTable'] ?? $tableName;

        if ($this->parser->hasInsertSelect($sql)) {
            $projectedSql = (new Insert\SelectProjection($this->parser, $this->identityAllocator, $this->insertSelectRenderer, $this->cteComposer))->render($sql, $identityTable, $tableColumns, $insertColumns, $columnDefaults, $identityStrategies, $existingRows);
            $projectedSql = (new Insert\UpsertProjection($this->parser, $this->upsertProjector))->projectUpsert($sql, $projectedSql, $tableName, $tableColumns, $tables);

            return $this->selectTransformer->transform($projectedSql, $tables);
        }

        $selectSql = (new Insert\ValueProjection($this->parser, $this->identityAllocator, $this->rowRenderer, $this->castRenderer))->render($sql, $tableName, $tableColumns, $insertColumns, $tables[$tableName] ?? []);
        $selectSql = (new Insert\UpsertProjection($this->parser, $this->upsertProjector))->projectUpsert($sql, $selectSql, $tableName, $tableColumns, $tables);

        return $this->selectTransformer->transform(
            $this->cteComposer->carryPrefix($sql, $selectSql),
            $tables,
        );
    }

    /**
     * Commits staged generated identity values after a successful rewrite.
     */
    public function commitRewriteState(): void
    {
        $this->identityAllocator->commitProjection();
    }
}
