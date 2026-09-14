<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Rewrite\Sampling;

use ZtdQuery\Exception\UnsupportedSqlException;
use ZtdQuery\Platform\Postgres\Sql\PgSqlIdentifierQuoter;
use ZtdQuery\Platform\Postgres\Sql\Sampling\PgSqlTableSample;
use ZtdQuery\Platform\Postgres\Sql\Sampling\PgSqlTableSampleParser;

/**
 * Table sample rewriter for PostgreSQL queries.
 */
final class PgSqlTableSampleRewriter
{
    private PgSqlTableSampleParser $parser;
    private PgSqlIdentifierQuoter $quoter;

    /**
     * Initializes the collaborators and state used by this table sample rewriter.
     */
    public function __construct()
    {
        $this->parser = new PgSqlTableSampleParser();
        $this->quoter = new PgSqlIdentifierQuoter();
    }

    /**
     * @param array<string, array<string, mixed>> $tables
     * @throws UnsupportedSqlException
     */
    public function rewrite(string $sql, array $tables): string
    {
        $samples = $this->parser->parse($sql);
        usort($samples, static fn (PgSqlTableSample $left, PgSqlTableSample $right): int => $right->startOffset <=> $left->startOffset);

        foreach ($samples as $index => $sample) {
            $columns = (new TableColumns())->columns($sample->tableName, $tables);
            if ($columns === []) {
                throw new UnsupportedSqlException(
                    $sql,
                    "Cannot determine columns for TABLESAMPLE source '{$sample->tableName}'",
                );
            }
            $replacement = (new SampleProjection($this->quoter))->replacement($sample, $columns, $index);
            $sql = substr_replace(
                $sql,
                $replacement,
                $sample->startOffset,
                $sample->endOffset - $sample->startOffset,
            );
        }

        return $sql;
    }
}
