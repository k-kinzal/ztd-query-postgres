<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Connection\Copy;

use ZtdQuery\Platform\CopyTarget;

/**
 * Target sql operations for PostgreSQL copy.
 *
 * @visibility root
 */
final class TargetSql
{
    /**
     * @param non-empty-list<string> $columns
     */
    public function columnListSql(array $columns): string
    {
        return implode(', ', array_map($this->quoteIdentifier(...), $columns));
    }

    /**
     * Relation sql.
     */
    public function relationSql(CopyTarget $target): string
    {
        return implode('.', array_map($this->quoteIdentifier(...), $target->relation));
    }

    /**
     * Quote identifier.
     */
    public function quoteIdentifier(string $identifier): string
    {
        return '"' . str_replace('"', '""', $identifier) . '"';
    }
}
