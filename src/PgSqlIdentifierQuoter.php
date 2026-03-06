<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres;

use ZtdQuery\Platform\IdentifierQuoter;

/**
 * PostgreSQL identifier quoting using double quotes.
 */
final class PgSqlIdentifierQuoter implements IdentifierQuoter
{
    /**
     * {@inheritDoc}
     */
    public function quote(string $identifier): string
    {
        $escaped = str_replace('"', '""', $identifier);

        return '"' . $escaped . '"';
    }
}
