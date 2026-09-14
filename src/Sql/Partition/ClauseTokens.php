<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Sql\Partition;

use ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile;
use ZtdQuery\Sql\SqlToken;
use ZtdQuery\Sql\SqlTokenKind;
use ZtdQuery\Sql\SqlTokenStream;

/**
 * Clause tokens operations for PostgreSQL partition.
 *
 * @visibility root
 */
final class ClauseTokens
{
    /**
     * @param list<SqlToken> $tokens
     * @return array{values: list<string>, next: int}|null
     */
    public function parenthesizedValues(string $sql, array $tokens, int $openIndex): ?array
    {
        $closeIndex = $this->closingParenthesisIndex($tokens, $openIndex);
        if ($closeIndex === null) {
            return null;
        }
        $open = $tokens[$openIndex];
        $close = $tokens[$closeIndex];
        $body = substr($sql, $open->endOffset(), $close->offset - $open->endOffset());
        $values = SqlTokenStream::tokenize($body, PgSqlLexerProfile::create())->splitTopLevel();

        return ['values' => $values, 'next' => $closeIndex + 1];
    }

    /**
     * @param list<SqlToken> $tokens
     */
    public function closingParenthesisIndex(array $tokens, int $openIndex): ?int
    {
        $open = $tokens[$openIndex] ?? null;
        if (!$open instanceof SqlToken) {
            return null;
        }
        if (!$this->isSymbol($open, '(')) {
            return null;
        }

        $afterOpen = false;
        foreach ($tokens as $index => $token) {
            if ($token === $open) {
                $afterOpen = true;
            } elseif ($afterOpen && $this->isSymbol($token, ')') && $token->depth === $open->depth) {
                return $index;
            }
        }

        return null;
    }

    /**
     * @param list<SqlToken> $tokens
     */
    public function keywordPairIndex(array $tokens, string $first, string $second): ?int
    {
        foreach ($tokens as $index => $token) {
            if (!$token->isTopLevel()) {
                continue;
            }
            if (!$token->isKeyword($first)) {
                continue;
            }
            $next = $tokens[$index + 1] ?? null;
            if (!$next instanceof SqlToken) {
                continue;
            }
            if (!$next->isKeyword($second)) {
                continue;
            }

            return $index;
        }

        return null;
    }

    /**
     * @param list<SqlToken> $tokens
     */
    public function keywordIndex(array $tokens, string $keyword): ?int
    {
        foreach ($tokens as $index => $token) {
            if ($token->isTopLevel() && $token->isKeyword($keyword)) {
                return $index;
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
        if ($token->kind === SqlTokenKind::Word) {
            $identifier['name'] = strtolower($identifier['name']);
        }

        $dot = $tokens[$identifier['next']] ?? null;
        while ($dot instanceof SqlToken && $this->isSymbol($dot, '.')) {
            $componentIndex = $identifier['next'] + 1;
            $component = $this->qualifiedIdentifierAt($stream, $tokens, $componentIndex);
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
}
