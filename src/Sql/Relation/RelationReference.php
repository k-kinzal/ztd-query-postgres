<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Sql\Relation;

use ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile;
use ZtdQuery\Sql\SqlToken;
use ZtdQuery\Sql\SqlTokenKind;
use ZtdQuery\Sql\SqlTokenStream;

/**
 * Relation reference operations for PostgreSQL relation.
 *
 * @visibility root
 */
final class RelationReference
{
    /**
     * @param list<SqlToken> $tokens
     */
    public function closingToken(array $tokens, int $openingIndex): ?SqlToken
    {
        for ($index = $openingIndex; isset($tokens[$index]); $index++) {
            $candidate = $tokens[$index];
            if ($candidate->text === ')' && $candidate->isTopLevel()) {
                return $candidate;
            }
        }

        return null;
    }

    /**
     * @param list<SqlToken> $tokens
     * @return array{name: string, start: int, unqualifiedStart: int, end: int}|null
     */
    public function referenceAt(string $sql, array $tokens, int $index): ?array
    {
        $token = $tokens[$index];
        if ($token->isKeyword('VALUES') || $token->isKeyword('SELECT') || $token->isKeyword('WITH')) {
            return null;
        }

        $component = $this->identifierComponentAt($tokens, $index);
        if ($component === null) {
            return null;
        }
        [$name, $nextIndex, $start, $unqualifiedStart, $end] = $component;

        while (($tokens[$nextIndex] ?? null)?->kind === SqlTokenKind::Symbol
            && $tokens[$nextIndex]->text === '.'
        ) {
            $component = $this->identifierComponentAt($tokens, $nextIndex + 1);
            if ($component === null) {
                break;
            }
            [$name, $nextIndex, , $unqualifiedStart, $end] = $component;
        }

        $next = $tokens[$nextIndex] ?? null;
        if ($next !== null && $next->kind === SqlTokenKind::Symbol && $next->text === '(') {
            return null;
        }

        return [
            'name' => $name,
            'start' => $start,
            'unqualifiedStart' => $unqualifiedStart,
            'end' => $end,
        ];
    }

    /**
     * @param list<SqlToken> $tokens
     * @return array{string, int, int, int, int}|null
     */
    public function identifierComponentAt(array $tokens, int $index): ?array
    {
        $token = $tokens[$index] ?? null;
        if ($token === null) {
            return null;
        }
        if ($token->kind === SqlTokenKind::Word) {
            return [$token->text, $index + 1, $token->offset, $token->offset, $token->endOffset()];
        }
        $name = PgSqlLexerProfile::create()->quotedIdentifierValue($token->text);
        if ($name === null) {
            return null;
        }

        return [
            $name,
            $index + 1,
            $token->offset,
            $token->offset,
            $token->endOffset(),
        ];
    }

    /**
     * @return list<SqlToken>
     */
    public function tokens(string $sql): array
    {
        return SqlTokenStream::tokenize($sql, PgSqlLexerProfile::create())->significantTokens();
    }
}
