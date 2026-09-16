<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Sql\Dml\Update;

use ZtdQuery\Sql\SqlTokenStream;

/**
 * Extracts PostgreSQL UPDATE aliases and assignments.
 *
 * @visibility root
 */
final class UpdateClauseParser
{
    /**
     * Extract table alias from UPDATE statement.
     */
    public function extractUpdateAlias(string $sql): ?string
    {
        $sql = \ZtdQuery\Platform\Postgres\Sql\PostgreSqlLexicalMasker::maskComments($sql);
        if (preg_match('/UPDATE\s+(?:ONLY\s+)?(?:"[^"]+"|[a-zA-Z_]\w*(?:\."[^"]+"|\.(?:[a-zA-Z_]\w*))?)\s+(?:AS\s+)?("[^"]+"|[a-zA-Z_]\w*)\s+SET\b/i', $sql, $m) === 1) {
            return (new \ZtdQuery\Platform\Postgres\Sql\Lexing\IdentifierDecoder())->unquoteIdentifier($m[1]);
        }

        return null;
    }

    /**
     * Extract SET assignments from UPDATE statement.
     *
     * @return array<string, string> column => value expression
     */
    public function extractUpdateSets(string $sql): array
    {
        $setClause = SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create())->topLevelClause(
            ['SET'],
            [['FROM'], ['WHERE'], ['RETURNING']],
        );
        if ($setClause === null) {
            return [];
        }

        $assignments = SqlTokenStream::tokenize($setClause, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create())->splitTopLevel();
        $result = [];

        foreach ($assignments as $assignment) {
            $assignment = trim($assignment);
            if (preg_match('/^("[^"]+"|[a-zA-Z_]\w*)\s*=\s*(.+)$/s', $assignment, $parts) === 1) {
                $colName = (new \ZtdQuery\Platform\Postgres\Sql\Lexing\IdentifierDecoder())->unquoteIdentifier($parts[1]);
                $result[$colName] = trim($parts[2]);
            }
        }

        return $result;
    }
}
