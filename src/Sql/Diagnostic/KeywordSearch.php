<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Sql\Diagnostic;

use ZtdQuery\Sql\SqlToken;

/**
 * Keyword search operations for PostgreSQL diagnostic.
 *
 * @visibility root
 */
final class KeywordSearch
{
    /**
     * @param list<SqlToken> $tokens
     * @param non-empty-list<string> $keywords
     */
    public static function containsKeyword(array $tokens, array $keywords): bool
    {
        foreach ($tokens as $token) {
            foreach ($keywords as $keyword) {
                if ($token->isKeyword($keyword)) {
                    return true;
                }
            }
        }

        return false;
    }
}
