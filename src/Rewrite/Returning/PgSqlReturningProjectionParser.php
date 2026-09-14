<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Rewrite\Returning;

use ZtdQuery\Exception\UnsupportedSqlException;
use ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile;
use ZtdQuery\Rewrite\ReturningProjection;
use ZtdQuery\Sql\SqlTokenStream;

/**
 * Returning projection parser for PostgreSQL queries.
 */
final class PgSqlReturningProjectionParser
{
    /**
     * Parses PostgreSQL SQL into the supported structural representation.
     * @throws UnsupportedSqlException
     */
    public function parse(string $sql): ?ReturningProjection
    {
        $clause = SqlTokenStream::tokenize($sql, PgSqlLexerProfile::create())->topLevelClause(['RETURNING']);
        if ($clause === null) {
            return null;
        }

        $items = [];
        foreach (SqlTokenStream::tokenize(rtrim($clause, "; \t\n\r\0\x0B"), PgSqlLexerProfile::create())->splitTopLevel() as $expression) {
            $item = (new \ZtdQuery\Platform\Postgres\Sql\Returning\ProjectionItem())->parseItem($expression);
            if ($item === null) {
                throw new UnsupportedSqlException(
                    $sql,
                    'RETURNING supports columns, qualified columns, aliases, and wildcard projections',
                );
            }
            $items[] = $item;
        }

        if ($items === []) {
            throw new UnsupportedSqlException($sql, 'RETURNING requires a projection');
        }

        return ReturningProjection::fromItems($items);
    }
}
