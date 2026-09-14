<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Sql\Cte;

use ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile;
use ZtdQuery\Sql\SqlTokenStream;

/**
 * Identifier references operations for PostgreSQL cte.
 *
 * @visibility root
 */
final class IdentifierReferences
{
    /**
     * References identifier.
     */
    public function referencesIdentifier(string $sql, string $identifier): bool
    {
        foreach (SqlTokenStream::tokenize($sql, PgSqlLexerProfile::create())->significantTokens() as $token) {
            $candidate = (new HeaderParser())->identifierName($token);
            if ($candidate !== null && strcasecmp($candidate, $identifier) === 0) {
                return true;
            }
        }

        return false;
    }

    /**
     * @param list<string> $identifiers
     */
    public function referencesAnyIdentifier(string $sql, array $identifiers): bool
    {
        foreach ($identifiers as $identifier) {
            if ($this->referencesIdentifier($sql, $identifier)) {
                return true;
            }
        }

        return false;
    }
}
