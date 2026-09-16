<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Sql\Dml\Delete;

/**
 * Extracts PostgreSQL DELETE aliases.
 *
 * @visibility root
 */
final class DeleteClauseParser
{
    /**
     * Extract table alias from DELETE statement.
     */
    public function extractDeleteAlias(string $sql): ?string
    {
        $sql = \ZtdQuery\Platform\Postgres\Sql\PostgreSqlLexicalMasker::maskComments($sql);
        if (preg_match('/DELETE\s+FROM\s+(?:ONLY\s+)?(?:"[^"]+"|[a-zA-Z_]\w*(?:\."[^"]+"|\.(?:[a-zA-Z_]\w*))?)\s+(?:AS\s+)?("[^"]+"|[a-zA-Z_]\w*)\s+(?:USING\b|WHERE\b|RETURNING\b|$)/i', $sql, $m) === 1) {
            return (new \ZtdQuery\Platform\Postgres\Sql\Lexing\IdentifierDecoder())->unquoteIdentifier($m[1]);
        }

        return null;
    }
}
