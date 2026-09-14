<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Rewrite\Transformer;

use ZtdQuery\Platform\CastRenderer;
use ZtdQuery\Platform\IdentifierQuoter;
use ZtdQuery\Platform\Postgres\Rewrite\Cte\PgSqlCteShadowComposer;
use ZtdQuery\Platform\Postgres\Rewrite\GeneratedColumn\PgSqlGeneratedColumnProjector;
use ZtdQuery\Platform\Postgres\Rewrite\Sampling\PgSqlTableSampleRewriter;
use ZtdQuery\Platform\Postgres\Sql\PgSqlIdentifierQuoter;
use ZtdQuery\Platform\Postgres\Sql\Value\PgSqlCastRenderer;
use ZtdQuery\Platform\ValueRenderer;
use ZtdQuery\Rewrite\SqlTransformer;
use ZtdQuery\Schema\ColumnDeclaration;

/**
 * Applies CTE shadowing to SELECT statements for PostgreSQL.
 *
 * Key differences from MySQL SelectTransformer:
 * - Uses double-quote identifiers ("table") instead of backticks
 * - Uses AS MATERIALIZED for CTE definition (PG 12+ inline prevention)
 * - Uses VALUES clause for multi-row CTEs instead of UNION ALL chains
 * - Uses WHERE FALSE for empty CTEs instead of FROM DUAL WHERE 0
 * - Uses PostgreSQL CAST types (INTEGER, TEXT, BOOLEAN, etc.)
 */
final class SelectTransformer implements SqlTransformer
{
    private CastRenderer $castRenderer;
    private IdentifierQuoter $quoter;
    private ValueRenderer $valueRenderer;
    private PgSqlCteShadowComposer $cteComposer;
    private PgSqlGeneratedColumnProjector $generatedColumnProjector;
    private PgSqlTableSampleRewriter $tableSampleRewriter;

    /**
     * Initializes the collaborators and state used by this select transformer.
     */
    public function __construct(
        ?CastRenderer $castRenderer = null,
        ?IdentifierQuoter $quoter = null,
        ?ValueRenderer $valueRenderer = null,
    ) {
        $this->castRenderer = $castRenderer ?? new PgSqlCastRenderer();
        $this->quoter = $quoter ?? new PgSqlIdentifierQuoter();
        $this->valueRenderer = $valueRenderer ?? new \ZtdQuery\Platform\Postgres\Sql\Value\PgSqlValueRenderer($this->castRenderer);
        $this->cteComposer = new PgSqlCteShadowComposer();
        $this->generatedColumnProjector = new PgSqlGeneratedColumnProjector();
        $this->tableSampleRewriter = new PgSqlTableSampleRewriter();
    }

    /**
     * {@inheritDoc}
     * @throws \ZtdQuery\Exception\UnsupportedSqlException when a shadow relation uses an unsupported TABLESAMPLE method.
     */
    public function transform(string $sql, array $tables): string
    {
        $sql = $this->tableSampleRewriter->rewrite($sql, $tables);
        $ctes = [];
        foreach ($tables as $tableName => $tableContext) {
            if (isset($tableContext['viewSql'])) {
                $ctes[$tableName] = $this->quoter->quote($tableName) . " AS MATERIALIZED ({$tableContext['viewSql']})";
                continue;
            }
            if (isset($tableContext['sourceSql'])) {
                $ctes[$tableName] = $this->quoter->quote($tableName) . " AS MATERIALIZED ({$tableContext['sourceSql']})";
                continue;
            }

            $rows = $tableContext['rows'];
            $columns = $tableContext['columns'];
            /**
             * @var array<string, ColumnDeclaration> $columnTypes
             */
            $columnTypes = $tableContext['columnTypes'];
            $generatedExpressions = $tableContext['generatedExpressions'] ?? [];

            if ($columns === [] && $rows !== []) {
                $columns = array_keys($rows[0]);
                foreach ($rows as $row) {
                    foreach (array_keys($row) as $column) {
                        if (!in_array($column, $columns, true)) {
                            $columns[] = $column;
                        }
                    }
                }
            }

            if ($columns === [] && $rows === []) {
                continue;
            }

            $ctes[$tableName] = (new Cte\RowSourceRenderer($this->castRenderer, $this->generatedColumnProjector, $this->quoter, $this->valueRenderer))->generateCte(
                $tableName,
                $rows,
                $columns,
                $columnTypes,
                $generatedExpressions,
            );
        }

        return $this->cteComposer->compose($sql, $ctes);
    }
}
