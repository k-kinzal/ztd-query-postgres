<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Sql\Sampling;

use ZtdQuery\Sql\SqlToken;

/**
 * Sample tokens operations for PostgreSQL sampling.
 *
 * @visibility root
 */
final class SampleTokens
{
    /**
     * @param list<SqlToken> $tokens
     */
    public function tokenAtOffset(array $tokens, int $offset): ?SqlToken
    {
        foreach ($tokens as $token) {
            if ($token->offset === $offset) {
                return $token;
            }
        }

        return null;
    }

    /**
     * @param list<SqlToken> $tokens
     */
    public function sampleIndexAfter(array $tokens, SqlToken $referenceToken): ?int
    {
        $afterReference = false;
        foreach ($tokens as $index => $token) {
            if ($token === $referenceToken) {
                $afterReference = true;
                continue;
            }
            if (!$afterReference || !$this->sameLevel($token, $referenceToken)) {
                continue;
            }
            if ($token->isKeyword('TABLESAMPLE')) {
                return $index;
            }
            if ($token->text === ',' || $this->isRelationBoundary($token)) {
                return null;
            }
        }

        return null;
    }

    /**
     * Is relation boundary.
     */
    public function isRelationBoundary(SqlToken $token): bool
    {
        foreach (['JOIN', 'ON', 'USING', 'WHERE', 'GROUP', 'HAVING', 'ORDER', 'LIMIT', 'OFFSET', 'UNION', 'INTERSECT', 'EXCEPT', 'FOR', 'RETURNING'] as $keyword) {
            if ($token->isKeyword($keyword)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Is opening parenthesis.
     */
    public function isOpeningParenthesis(SqlToken $token, SqlToken $referenceToken): bool
    {
        return $token->text === '(' && $this->sameLevel($token, $referenceToken);
    }

    /**
     * @param list<SqlToken> $tokens
     */
    public function closingParenthesisIndex(array $tokens, int $openIndex): ?int
    {
        $open = $tokens[$openIndex] ?? null;
        if ($open === null) {
            return null;
        }
        $afterOpen = false;
        foreach ($tokens as $index => $token) {
            if ($token === $open) {
                $afterOpen = true;
            } elseif ($afterOpen && $token->text === ')' && $this->sameLevel($token, $open)) {
                return $index;
            }
        }

        return null;
    }

    /**
     * @param list<SqlToken> $tokens
     */
    public function tokenAfter(array $tokens, SqlToken $referenceToken): SqlToken
    {
        $afterReference = false;
        foreach ($tokens as $token) {
            if ($afterReference) {
                return $token;
            }
            $afterReference = $token === $referenceToken;
        }

        return $referenceToken;
    }

    /**
     * Same level.
     */
    public function sameLevel(SqlToken $token, SqlToken $referenceToken): bool
    {
        return $token->depth === $referenceToken->depth
            && $token->bracketDepth === $referenceToken->bracketDepth;
    }
}
