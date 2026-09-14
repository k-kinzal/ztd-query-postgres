<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Schema\Definition;

use ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile;
use ZtdQuery\Sql\SqlToken;
use ZtdQuery\Sql\SqlTokenKind;
use ZtdQuery\Sql\SqlTokenStream;

/**
 * Table body operations for PostgreSQL definition.
 *
 * @visibility root
 */
final class TableBody
{
    /**
     * Table body.
     */
    public function tableBody(string $sql): ?string
    {
        $stream = SqlTokenStream::tokenize($sql, PgSqlLexerProfile::create());
        $tokens = $stream->significantTokens();
        $open = $this->openingParenthesis($stream, $tokens);
        if ($open === null) {
            return null;
        }
        foreach ($tokens as $token) {
            if ($this->isSymbol($token, ')') && $token->depth === $open->depth) {
                return substr($sql, $open->endOffset(), $token->offset - $open->endOffset());
            }
        }

        return null;
    }

    /**
     * @param list<SqlToken> $tokens
     * @return array{name: string, next: int}|null
     */
    public function qualifiedIdentifierAt(SqlTokenStream $stream, array $tokens, int $index): ?array
    {
        $token = $tokens[$index] ?? null;
        if (!$token instanceof SqlToken) {
            return null;
        }
        if (!in_array($token->kind, [SqlTokenKind::Word, SqlTokenKind::QuotedIdentifier], true)) {
            return null;
        }
        $identifier = $stream->identifierAt($index);
        if ($identifier === null) {
            return null;
        }

        $dot = $tokens[$identifier['next']] ?? null;
        while ($dot instanceof SqlToken && $this->isSymbol($dot, '.')) {
            $component = $this->qualifiedIdentifierAt($stream, $tokens, $identifier['next'] + 1);
            if ($component === null) {
                return null;
            }
            $identifier = $component;
            $dot = $tokens[$identifier['next']] ?? null;
        }

        return $identifier;
    }

    /**
     * Is symbol.
     */
    public function isSymbol(SqlToken $token, string $symbol): bool
    {
        return $token->kind === SqlTokenKind::Symbol && $token->text === $symbol;
    }

    /**
     * Split table body by top-level commas (respecting parentheses).
     *
     * @return list<string>
     */
    public function splitTableBody(string $body): array
    {
        $entries = [];
        $current = '';
        $depth = 0;
        $len = strlen($body);

        for ($i = 0; $i < $len; $i++) {
            $char = $body[$i];

            if ($char === "'" || $char === '"') {
                $length = \ZtdQuery\Platform\Postgres\Sql\Lexing\QuotedSpan::quotedLength(substr($body, $i), $char, false);
                $current .= substr($body, $i, $length);
                $i += $length - 1;
                continue;
            }

            if ($char === '(') {
                $depth++;
                $current .= $char;
                continue;
            }

            if ($char === ')') {
                $depth--;
                $current .= $char;
                continue;
            }

            if ($char === ',' && $depth === 0) {
                $entries[] = trim($current);
                $current = '';
                continue;
            }

            $current .= $char;
        }

        $val = trim($current);
        if ($val !== '') {
            $entries[] = $val;
        }

        return $entries;
    }
    /**
     * Resolves CREATE TABLE, optional existence guards and its qualified identifier.
     * @param list<SqlToken> $tokens
     */
    public function openingParenthesis(SqlTokenStream $stream, array $tokens): ?SqlToken
    {
        $create = $tokens[0] ?? null;
        if (!$create instanceof SqlToken || !$create->isKeyword('CREATE')) {
            return null;
        }

        $tableIndex = null;
        foreach ($tokens as $index => $token) {
            if ($token->isTopLevel() && $token->isKeyword('TABLE')) {
                $tableIndex = $index;
            }
        }
        if ($tableIndex === null) {
            return null;
        }

        $index = $tableIndex + 1;
        $candidate = $tokens[$index] ?? null;
        if (!$candidate instanceof SqlToken) {
            return null;
        }
        $index = $this->skipExistenceGuard($tokens, $index);
        if ($index === null) {
            return null;
        }
        $identifier = $this->qualifiedIdentifierAt($stream, $tokens, $index);
        if ($identifier === null) {
            return null;
        }

        $open = $tokens[$identifier['next']] ?? null;
        if (!$open instanceof SqlToken) {
            return null;
        }
        if (!$this->isSymbol($open, '(')) {
            return null;
        }

        return $open;
    }

    /**
     * Skips a complete IF NOT EXISTS guard and rejects malformed guards.
     * @param list<SqlToken> $tokens
     */
    public function skipExistenceGuard(array $tokens, int $index): ?int
    {
        if (($tokens[$index] ?? null)?->isKeyword('IF') === true) {
            $not = $tokens[$index + 1] ?? null;
            $exists = $tokens[$index + 2] ?? null;
            if (!$not instanceof SqlToken || !$not->isKeyword('NOT')) {
                return null;
            }
            if (!$exists instanceof SqlToken || !$exists->isKeyword('EXISTS')) {
                return null;
            }
            $index += 3;
        }
        return $index;
    }
}
