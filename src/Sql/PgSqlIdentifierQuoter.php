<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Sql;

use ZtdQuery\Platform\IdentifierQuoter;

/**
 * PostgreSQL identifier quoting using double quotes.
 *
 * @visibility public
 * @example Quote reserved identifiers
 *     (new \ZtdQuery\Platform\Postgres\Sql\PgSqlIdentifierQuoter())->quote('select') // => '"select"'
 */
final class PgSqlIdentifierQuoter implements IdentifierQuoter
{
    /**
     * {@inheritDoc}
     *
     * @visibility public
     * @example Quote an identifier containing a double quote
     *     (new \ZtdQuery\Platform\Postgres\Sql\PgSqlIdentifierQuoter())->quote('order"items') // => '"order""items"'
     */
    public function quote(string $identifier): string
    {
        $escaped = str_replace('"', '""', $identifier);

        return '"' . $escaped . '"';
    }
}
