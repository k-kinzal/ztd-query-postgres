<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres;

use ZtdQuery\Exception\UnsupportedSqlException;
use ZtdQuery\Platform\Postgres\Transformer\DeleteTransformer;
use ZtdQuery\Platform\Postgres\Transformer\InsertTransformer;
use ZtdQuery\Platform\Postgres\Transformer\MergeTransformer;
use ZtdQuery\Platform\Postgres\Transformer\SelectTransformer;
use ZtdQuery\Platform\Postgres\Transformer\UpdateTransformer;
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
    private ?MergeTransformer $mergeTransformer = null;

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
    }

    /**
     * {@inheritDoc}
     */
    public function transform(string $sql, array $tables): string
    {
        $type = $this->parser->classifyStatement($sql);

        return match ($type) {
            'SELECT' => $this->selectTransformer->transform($sql, $tables),
            'INSERT' => $this->insertTransformer->transform($sql, $tables),
            'UPDATE' => $this->updateTransformer->transform($sql, $tables),
            'DELETE' => $this->deleteTransformer->transform($sql, $tables),
            'MERGE' => $this->mergeTransformer()->transform($sql, $tables),
            default => throw new UnsupportedSqlException($sql, 'Statement type not supported by transformer'),
        };
    }

    public function commitRewriteState(): void
    {
        $this->insertTransformer->commitRewriteState();
    }

    private function mergeTransformer(): MergeTransformer
    {
        if ($this->mergeTransformer === null) {
            $this->mergeTransformer = new MergeTransformer(new PgSqlMergeParser(), $this->selectTransformer);
        }

        return $this->mergeTransformer;
    }
}
