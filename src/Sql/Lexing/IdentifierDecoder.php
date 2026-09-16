<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Sql\Lexing;

use ZtdQuery\Sql\SqlTokenKind;
use ZtdQuery\Sql\SqlTokenStream;

/**
 * Decodes quoted PostgreSQL identifiers and column lists.
 *
 * @visibility root
 */
final class IdentifierDecoder
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
}
