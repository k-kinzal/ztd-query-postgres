<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Sql\Statement;

/**
 * Extracts PostgreSQL DML target table names.
 *
 * @visibility root
 */
final class TargetTableParser
{
    /**
     * Extract table name from INSERT statement.
     */
    public function extractInsertTable(string $sql): ?string
    {
        $sql = \ZtdQuery\Platform\Postgres\Sql\PostgreSqlLexicalMasker::maskComments($sql);
        if (preg_match('/INSERT\s+INTO\s+(?:ONLY\s+)?("[^"]+"|[a-zA-Z_]\w*(?:\."[^"]+"|\.(?:[a-zA-Z_]\w*))?)(?:\s+AS\s+"?(\w+)"?)?/i', $sql, $m) === 1) {
            return (new \ZtdQuery\Platform\Postgres\Sql\Lexing\IdentifierDecoder())->unquoteIdentifier((new \ZtdQuery\Platform\Postgres\Sql\Lexing\IdentifierDecoder())->stripSchemaPrefix($m[1]));
        }

        return null;
    }

    /**
     * Extract table name from UPDATE statement.
     */
    public function extractUpdateTable(string $sql): ?string
    {
        $sql = \ZtdQuery\Platform\Postgres\Sql\PostgreSqlLexicalMasker::maskComments($sql);
        if (preg_match('/UPDATE\s+(?:ONLY\s+)?("[^"]+"|[a-zA-Z_]\w*(?:\."[^"]+"|\.(?:[a-zA-Z_]\w*))?)(?:\s+(?:AS\s+)?("[^"]+"|[a-zA-Z_]\w*))?/i', $sql, $m) === 1) {
            return (new \ZtdQuery\Platform\Postgres\Sql\Lexing\IdentifierDecoder())->unquoteIdentifier((new \ZtdQuery\Platform\Postgres\Sql\Lexing\IdentifierDecoder())->stripSchemaPrefix($m[1]));
        }

        return null;
    }

    /**
     * Extract table name from DELETE statement.
     */
    public function extractDeleteTable(string $sql): ?string
    {
        $sql = \ZtdQuery\Platform\Postgres\Sql\PostgreSqlLexicalMasker::maskComments($sql);
        if (preg_match('/DELETE\s+FROM\s+(?:ONLY\s+)?("[^"]+"|[a-zA-Z_]\w*(?:\."[^"]+"|\.(?:[a-zA-Z_]\w*))?)(?:\s+(?:AS\s+)?("[^"]+"|[a-zA-Z_]\w*))?/i', $sql, $m) === 1) {
            return (new \ZtdQuery\Platform\Postgres\Sql\Lexing\IdentifierDecoder())->unquoteIdentifier((new \ZtdQuery\Platform\Postgres\Sql\Lexing\IdentifierDecoder())->stripSchemaPrefix($m[1]));
        }

        return null;
    }
}
