<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Sql\Relation;

use ZtdQuery\Sql\SqlToken;
use ZtdQuery\Sql\SqlTokenKind;

/**
 * From clause operations for PostgreSQL relation.
 *
 * @visibility root
 */
final class FromClause
{
    /**
     * @return list<array{name: string, start: int, unqualifiedStart: int, end: int}>
     */
    public function referencesFromClause(string $clause): array
    {
        $tokens = (new RelationReference())->tokens($clause);
        $references = [];
        $expectSource = true;

        foreach ($tokens as $index => $token) {
            if (!$token->isTopLevel()) {
                continue;
            }
            if ($token->isKeyword('JOIN')) {
                $expectSource = true;
                continue;
            }
            if ($token->kind === SqlTokenKind::Symbol && $token->text === ',') {
                $expectSource = true;
                continue;
            }
            if (!$expectSource) {
                continue;
            }
            if ($token->isKeyword('LATERAL') || $token->isKeyword('ONLY')) {
                continue;
            }

            if ($token->kind === SqlTokenKind::Symbol && $token->text === '(') {
                $references = array_merge($references, $this->parenthesizedReferences($clause, $tokens, $index, $token));
                continue;
            }

            $expectSource = false;
            $reference = (new RelationReference())->referenceAt($clause, $tokens, $index);
            if ($reference !== null) {
                $references[] = $reference;
            }
        }

        return $references;
    }

    /**
     * @param list<SqlToken> $tokens
     */
    public function findFromEnd(string $sql, array $tokens, SqlToken $fromToken): int
    {
        $terminators = [
            ['WHERE'], ['GROUP', 'BY'], ['HAVING'], ['WINDOW'], ['ORDER', 'BY'],
            ['LIMIT'], ['OFFSET'], ['FETCH'], ['FOR'], ['RETURNING'],
            ['UNION'], ['INTERSECT'], ['EXCEPT'],
        ];
        $afterFrom = false;
        foreach ($tokens as $index => $token) {
            if (!$afterFrom) {
                $afterFrom = $token === $fromToken;
                continue;
            }
            if ($token->depth < $fromToken->depth || $token->bracketDepth < $fromToken->bracketDepth) {
                return $token->offset;
            }
            if ($token->depth !== $fromToken->depth || $token->bracketDepth !== $fromToken->bracketDepth) {
                continue;
            }
            foreach ($terminators as $sequence) {
                if ($this->matchesKeywordSequence($tokens, $index, $sequence)) {
                    return $token->offset;
                }
            }
        }

        return strlen($sql);
    }

    /**
     * @param list<SqlToken> $tokens
     * @param non-empty-list<string> $keywords
     */
    public function matchesKeywordSequence(array $tokens, int $index, array $keywords): bool
    {
        foreach ($keywords as $relative => $keyword) {
            $candidate = $tokens[$index + $relative] ?? null;
            if ($candidate === null || !$candidate->isKeyword($keyword)) {
                return false;
            }
        }

        return true;
    }
    /**
     * Recurses into joined relations while leaving subquery scopes to their own parser.
     * @param list<SqlToken> $tokens
     * @return list<array{name: string, start: int, unqualifiedStart: int, end: int}>
     */
    public function parenthesizedReferences(string $clause, array $tokens, int $index, SqlToken $token): array
    {
        $references = [];
        $closingToken = (new RelationReference())->closingToken($tokens, $index);
        if ($closingToken === null) {
            return [];
        }
        $innerStart = $token->endOffset();
        $inner = substr($clause, $innerStart, $closingToken->offset - $innerStart);
        $innerTokens = (new RelationReference())->tokens($inner);
        if ($innerTokens === []) {
            return [];
        }
        if ($innerTokens[0]->isKeyword('SELECT')
            || $innerTokens[0]->isKeyword('WITH')
            || $innerTokens[0]->isKeyword('VALUES')
        ) {
            return [];
        }
        foreach ($this->referencesFromClause($inner) as $reference) {
            $references[] = [
                'name' => $reference['name'],
                'start' => $innerStart + $reference['start'],
                'unqualifiedStart' => $innerStart + $reference['unqualifiedStart'],
                'end' => $innerStart + $reference['end'],
            ];
        }
        return $references;
    }
}
