<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Rewrite\Transformer\Insert;

use ZtdQuery\Exception\UnsupportedSqlException;
use ZtdQuery\Platform\Postgres\Rewrite\Cte\PgSqlCteShadowComposer;
use ZtdQuery\Platform\Postgres\Rewrite\Transformer\InsertSelectRenderer;
use ZtdQuery\Platform\Postgres\Sql\PgSqlParser;
use ZtdQuery\Rewrite\ShadowIdentityAllocator;

/**
 * Builds the result-select projection of an INSERT SELECT source.
 *
 * @visibility root
 */
final class SelectProjection
{
    /**
     * Shares the INSERT transformer's staged identity state and prefix renderer.
     */
    public function __construct(private readonly PgSqlParser $parser, private readonly ShadowIdentityAllocator $identityAllocator, private readonly InsertSelectRenderer $insertSelectRenderer, private readonly PgSqlCteShadowComposer $cteComposer)
    {
    }

    /**
     * Projects an INSERT SELECT source with generated identity starting values.
     * @template T
     * @param list<string> $tableColumns
     * @param list<string> $insertColumns
     * @param array<string, string> $columnDefaults
     * @param array<string, \ZtdQuery\Schema\Key\IdentityGenerationStrategy> $identityStrategies
     * @param array<int, array<string, T>> $existingRows
     * @throws UnsupportedSqlException
     */
    public function render(string $sql, string $identityTable, array $tableColumns, array $insertColumns, array $columnDefaults, array $identityStrategies, array $existingRows): string
    {
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
        return $projectedSql;
    }
}
