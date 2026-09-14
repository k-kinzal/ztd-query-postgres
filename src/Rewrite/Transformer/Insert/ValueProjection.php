<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Rewrite\Transformer\Insert;

use ZtdQuery\Exception\UnsupportedSqlException;
use ZtdQuery\Platform\CastRenderer;
use ZtdQuery\Platform\Postgres\Rewrite\Transformer\InsertRowRenderer;
use ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile;
use ZtdQuery\Platform\Postgres\Sql\PgSqlParser;
use ZtdQuery\Rewrite\ShadowIdentityAllocator;
use ZtdQuery\Schema\ColumnDeclaration;
use ZtdQuery\Sql\SqlTokenStream;

/**
 * Converts VALUES rows into their typed result-select projection.
 *
 * @visibility root
 */
final class ValueProjection
{
    /**
     * Shares the staged identity allocator with the owning INSERT transformer.
     */
    public function __construct(private readonly PgSqlParser $parser, private readonly ShadowIdentityAllocator $identityAllocator, private readonly InsertRowRenderer $rowRenderer, private readonly CastRenderer $castRenderer)
    {
    }

    /**
     * Projects each VALUES row with defaults, generated identities and declared casts.
     * @template T
     * @param list<string> $tableColumns
     * @param list<string> $insertColumns
     * @param array{rows?: array<int, array<string, T>>, columnTypes?: array<string, ColumnDeclaration>, columnDefaults?: array<string, string>, identityStrategies?: array<string, \ZtdQuery\Schema\Key\IdentityGenerationStrategy>, storageTable?: string} $table
     * @throws UnsupportedSqlException
     */
    public function render(string $sql, string $tableName, array $tableColumns, array $insertColumns, array $table): string
    {
        $identityTable = $table['storageTable'] ?? $tableName;
        $identityStrategies = $table['identityStrategies'] ?? [];
        $existingRows = $table['rows'] ?? [];
        $columnDefaults = $table['columnDefaults'] ?? [];
        $valueRows = SqlTokenStream::tokenize($sql, PgSqlLexerProfile::create())->topLevelClause(['DEFAULT', 'VALUES']) !== null
            ? [[]]
            : $this->parser->extractInsertValues($sql);
        if ($valueRows === []) {
            throw new UnsupportedSqlException($sql, 'Cannot extract INSERT values');
        }

        $selectParts = [];
        $columnTypes = $table['columnTypes'] ?? [];
        foreach ($valueRows as $values) {
            $sourceColumns = $insertColumns !== [] || $values === [] ? $insertColumns : $tableColumns;
            if (count($sourceColumns) !== count($values)) {
                throw new UnsupportedSqlException($sql, 'Insert values count does not match column count');
            }
            $providedExpressions = $this->rowRenderer->providedExpressions($sourceColumns, $values);
            $generatedValues = $this->identityAllocator->allocateMissing(
                $identityTable,
                $identityStrategies,
                array_keys($providedExpressions),
                $existingRows,
            );
            $projected = $this->rowRenderer->render($tableColumns, $providedExpressions, $columnDefaults, $generatedValues);

            $selectParts[] = (new ExpressionCast($this->castRenderer))->projection($projected, $columnTypes);
        }

        $selectSql = implode(' UNION ALL ', $selectParts);
        return $selectSql;
    }
}
