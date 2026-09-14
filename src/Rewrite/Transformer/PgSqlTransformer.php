<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Rewrite\Transformer;

use ZtdQuery\Exception\UnsupportedSqlException;
use ZtdQuery\Platform\Postgres\Sql\Merge\PgSqlMergeParser;
use ZtdQuery\Platform\Postgres\Sql\PgSqlParser;
use ZtdQuery\Rewrite\SqlTransformer;

/**
 * Composite SQL transformer for PostgreSQL.
 *
 * Classifies the SQL statement type and delegates to the appropriate
 * sub-transformer for PostgreSQL-specific transformation.
 */
final class PgSqlTransformer implements SqlTransformer
{
    private PgSqlParser $parser;
    private SelectTransformer $selectTransformer;
    private InsertTransformer $insertTransformer;
    private UpdateTransformer $updateTransformer;
    private DeleteTransformer $deleteTransformer;
    private MergeTransformer $mergeTransformer;

    /**
     * Initializes the collaborators and state used by this transformer.
     */
    public function __construct(
        PgSqlParser $parser,
        SelectTransformer $selectTransformer,
        InsertTransformer $insertTransformer,
        UpdateTransformer $updateTransformer,
        DeleteTransformer $deleteTransformer
    ) {
        $this->parser = $parser;
        $this->selectTransformer = $selectTransformer;
        $this->insertTransformer = $insertTransformer;
        $this->updateTransformer = $updateTransformer;
        $this->deleteTransformer = $deleteTransformer;
        $this->mergeTransformer = new MergeTransformer(new PgSqlMergeParser(), $selectTransformer);
    }

    /**
     * {@inheritDoc}
     * @throws UnsupportedSqlException
     */
    public function transform(string $sql, array $tables): string
    {
        $type = $this->parser->classifyStatement($sql);

        return match ($type) {
            'SELECT' => $this->selectTransformer->transform($sql, $tables),
            'INSERT' => $this->insertTransformer->transform($sql, $tables),
            'UPDATE' => $this->updateTransformer->transform($sql, $tables),
            'DELETE' => $this->deleteTransformer->transform($sql, $tables),
            'MERGE' => $this->mergeTransformer->transform($sql, $tables),
            'TRUNCATE', 'CREATE_TABLE', 'DROP_TABLE', 'ALTER_TABLE', 'DO', 'TCL', null => throw new UnsupportedSqlException($sql, 'Statement type not supported by transformer'),
        };
    }

    /**
     * Commits staged generated identity values after a successful rewrite.
     */
    public function commitRewriteState(): void
    {
        $this->insertTransformer->commitRewriteState();
    }
}
