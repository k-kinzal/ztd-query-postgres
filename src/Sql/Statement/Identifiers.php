<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Sql\Statement;

use ZtdQuery\Sql\SqlTokenKind;
use ZtdQuery\Sql\SqlTokenStream;

/**
 * Identifiers operations for PostgreSQL statement.
 *
 * @visibility root
 */
final class Identifiers
{
    /**
     * Unquote a PostgreSQL identifier (remove double quotes).
     */
    public function unquoteIdentifier(string $identifier): string
    {
        if (str_starts_with($identifier, '"') && str_ends_with($identifier, '"')) {
            $inner = substr($identifier, 1, -1);

            return str_replace('""', '"', $inner);
        }

        return $identifier;
    }

    /**
     * Strip schema prefix from a potentially schema-qualified name.
     * "public"."users" -> "users", public.users -> users
     */
    public function stripSchemaPrefix(string $name): string
    {
        if (preg_match('/^"[^"]+"\.(.+)$/', $name, $m) === 1) {
            return $m[1];
        }
        if (preg_match('/^[a-zA-Z_]\w*\.(.+)$/', $name, $m) === 1) {
            return $m[1];
        }

        return $name;
    }

    /**
     * @return list<string>
     */
    public function parseColumnList(string $columnStr): array
    {
        $columns = [];
        $parts = explode(',', $columnStr);
        foreach ($parts as $part) {
            $col = trim($part);
            $col = $this->unquoteIdentifier($col);
            if ($col !== '') {
                $columns[] = $col;
            }
        }

        return $columns;
    }

    /**
     * @return array{name: string, next: int}|null
     */
    public function truncateIdentifierAt(SqlTokenStream $stream, int $index): ?array
    {
        $tokens = $stream->significantTokens();
        $prefix = $tokens[$index] ?? null;
        if ($prefix === null) {
            return null;
        }
        if ($prefix->isKeyword('U')) {
            $ampersand = $tokens[$index + 1] ?? null;
            if ($ampersand !== null && $ampersand->text === '&') {
                $quotedIdentifier = $tokens[$index + 2] ?? null;
                if ($quotedIdentifier === null || $quotedIdentifier->kind !== SqlTokenKind::QuotedIdentifier) {
                    return null;
                }

                return [
                    'name' => $this->unquoteIdentifier($quotedIdentifier->text),
                    'next' => $index + 3,
                ];
            }
        }

        return $stream->identifierAt($index);
    }

    /**
     * Extract table name from INSERT statement.
     */
    public function extractInsertTable(string $sql): ?string
    {
        $sql = \ZtdQuery\Platform\Postgres\Sql\PostgreSqlLexicalMasker::maskComments($sql);
        if (preg_match('/INSERT\s+INTO\s+(?:ONLY\s+)?("[^"]+"|[a-zA-Z_]\w*(?:\."[^"]+"|\.(?:[a-zA-Z_]\w*))?)(?:\s+AS\s+"?(\w+)"?)?/i', $sql, $m) === 1) {
            return $this->unquoteIdentifier($this->stripSchemaPrefix($m[1]));
        }

        return null;
    }

    /**
     * Extract column list from INSERT statement.
     *
     * @return list<string>
     */
    public function extractInsertColumns(string $sql): array
    {
        $sql = \ZtdQuery\Platform\Postgres\Sql\PostgreSqlLexicalMasker::maskComments($sql);
        if (preg_match('/INSERT\s+INTO\s+(?:ONLY\s+)?(?:"[^"]+"|[a-zA-Z_]\w*(?:\."[^"]+"|\.(?:[a-zA-Z_]\w*))?)\s*\(([^)]+)\)\s*(?:VALUES|SELECT|DEFAULT)/i', $sql, $m) === 1) {
            return $this->parseColumnList($m[1]);
        }

        return [];
    }

    /**
     * Extract table name from UPDATE statement.
     */
    public function extractUpdateTable(string $sql): ?string
    {
        $sql = \ZtdQuery\Platform\Postgres\Sql\PostgreSqlLexicalMasker::maskComments($sql);
        if (preg_match('/UPDATE\s+(?:ONLY\s+)?("[^"]+"|[a-zA-Z_]\w*(?:\."[^"]+"|\.(?:[a-zA-Z_]\w*))?)(?:\s+(?:AS\s+)?("[^"]+"|[a-zA-Z_]\w*))?/i', $sql, $m) === 1) {
            return $this->unquoteIdentifier($this->stripSchemaPrefix($m[1]));
        }

        return null;
    }

    /**
     * Extract table alias from UPDATE statement.
     */
    public function extractUpdateAlias(string $sql): ?string
    {
        $sql = \ZtdQuery\Platform\Postgres\Sql\PostgreSqlLexicalMasker::maskComments($sql);
        if (preg_match('/UPDATE\s+(?:ONLY\s+)?(?:"[^"]+"|[a-zA-Z_]\w*(?:\."[^"]+"|\.(?:[a-zA-Z_]\w*))?)\s+(?:AS\s+)?("[^"]+"|[a-zA-Z_]\w*)\s+SET\b/i', $sql, $m) === 1) {
            return $this->unquoteIdentifier($m[1]);
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
                $colName = $this->unquoteIdentifier($parts[1]);
                $result[$colName] = trim($parts[2]);
            }
        }

        return $result;
    }

    /**
     * Extract table name from DELETE statement.
     */
    public function extractDeleteTable(string $sql): ?string
    {
        $sql = \ZtdQuery\Platform\Postgres\Sql\PostgreSqlLexicalMasker::maskComments($sql);
        if (preg_match('/DELETE\s+FROM\s+(?:ONLY\s+)?("[^"]+"|[a-zA-Z_]\w*(?:\."[^"]+"|\.(?:[a-zA-Z_]\w*))?)(?:\s+(?:AS\s+)?("[^"]+"|[a-zA-Z_]\w*))?/i', $sql, $m) === 1) {
            return $this->unquoteIdentifier($this->stripSchemaPrefix($m[1]));
        }

        return null;
    }

    /**
     * Extract table alias from DELETE statement.
     */
    public function extractDeleteAlias(string $sql): ?string
    {
        $sql = \ZtdQuery\Platform\Postgres\Sql\PostgreSqlLexicalMasker::maskComments($sql);
        if (preg_match('/DELETE\s+FROM\s+(?:ONLY\s+)?(?:"[^"]+"|[a-zA-Z_]\w*(?:\."[^"]+"|\.(?:[a-zA-Z_]\w*))?)\s+(?:AS\s+)?("[^"]+"|[a-zA-Z_]\w*)\s+(?:USING\b|WHERE\b|RETURNING\b|$)/i', $sql, $m) === 1) {
            return $this->unquoteIdentifier($m[1]);
        }

        return null;
    }
}
