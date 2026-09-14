<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Rewrite\Transformer;

use ZtdQuery\Exception\UnsupportedSqlException;
use ZtdQuery\Platform\Postgres\Rewrite\Cte\PgSqlCteShadowComposer;
use ZtdQuery\Platform\Postgres\Rewrite\GeneratedColumn\PgSqlGeneratedColumnProjector;
use ZtdQuery\Platform\Postgres\Sql\Merge\PgSqlMergeParser;
use ZtdQuery\Platform\Postgres\Sql\PgSqlIdentifierQuoter;
use ZtdQuery\Rewrite\SqlTransformer;

/**
 * Merge transformer for PostgreSQL queries.
 */
final class MergeTransformer implements SqlTransformer
{
    private PgSqlIdentifierQuoter $quoter;
    private InsertRowRenderer $rowRenderer;
    private PgSqlGeneratedColumnProjector $generatedColumnProjector;
    private PgSqlCteShadowComposer $cteComposer;
    private InsertSelectRenderer $insertSelectRenderer;

    /**
     * Initializes the collaborators and state used by this merge transformer.
     */
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
     * @throws UnsupportedSqlException
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
        $effectiveConditions = (new Merge\MatchConditions())->effectiveConditions($statement);
        $parts = [];
        $parts[] = (new Merge\RowActions($this->insertSelectRenderer, $this->quoter, $this->rowRenderer))->unchangedRows($statement, $columns, $effectiveConditions);

        $parts = array_merge($parts, (new Merge\RowActions($this->insertSelectRenderer, $this->quoter, $this->rowRenderer))->modifiedRows($sql, $statement, $columns, $defaults, $identityStrategies, $existingRows, $effectiveConditions));

        $resultSql = implode(' UNION ALL ', $parts);
        $resultSql = $this->generatedColumnProjector->project($resultSql, $columns, $generatedExpressions);
        $resultSql = $this->cteComposer->carryPrefix($sql, $resultSql);

        return $this->selectTransformer->transform($resultSql, $tables);
    }
}
