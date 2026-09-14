<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Sql\Diagnostic;

use ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile;
use ZtdQuery\Sql\SqlTokenStream;

/**
 * Read only diagnostic statement for PostgreSQL queries.
 */
final class PgSqlReadOnlyDiagnosticStatement
{
    /**
     * @var non-empty-list<string>
     */
    private const WRITE_KEYWORDS = [
        'INSERT', 'UPDATE', 'DELETE', 'MERGE',
        'CREATE', 'ALTER', 'DROP', 'TRUNCATE',
        'COPY', 'CALL', 'DO', 'EXECUTE',
        'GRANT', 'REVOKE', 'VACUUM',
    ];

    /**
     * Checks whether a diagnostic statement can execute without modifying tables.
     */
    public static function isSafe(string $sql): bool
    {
        $stream = SqlTokenStream::tokenize($sql, PgSqlLexerProfile::create());
        if (count($stream->splitStatements()) !== 1) {
            return false;
        }

        $tokens = $stream->significantTokens();
        if ($tokens === []) {
            return false;
        }
        if ($tokens[0]->isKeyword('SHOW')) {
            return count($tokens) > 1;
        }
        if (!$tokens[0]->isKeyword('EXPLAIN') || count($tokens) === 1) {
            return false;
        }
        if (!KeywordSearch::containsKeyword($tokens, ['ANALYZE', 'ANALYSE'])) {
            return true;
        }

        return !KeywordSearch::containsKeyword($tokens, self::WRITE_KEYWORDS);
    }
}
