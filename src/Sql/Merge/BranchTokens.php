<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Sql\Merge;

use ZtdQuery\Sql\SqlToken;

/**
 * Branch tokens operations for PostgreSQL merge.
 *
 * @visibility root
 */
final class BranchTokens
{
    /**
     * @param list<SqlToken> $tokens
     */
    public function keywordIndexAfter(array $tokens, string $keyword, SqlToken $anchor): ?int
    {
        $afterAnchor = false;
        foreach ($tokens as $index => $token) {
            if ($token === $anchor) {
                $afterAnchor = true;
                continue;
            }
            if ($afterAnchor && $token->isTopLevel() && $token->isKeyword($keyword)) {
                return $index;
            }
        }

        return null;
    }

    /**
     * @param list<SqlToken> $tokens
     */
    public function lastKeywordBetween(
        array $tokens,
        string $keyword,
        SqlToken $start,
        SqlToken $end,
    ): ?SqlToken {
        $found = null;
        $withinRange = false;
        foreach ($tokens as $token) {
            if ($token === $start) {
                $withinRange = true;
                continue;
            }
            if ($token === $end) {
                break;
            }
            if ($withinRange && $token->isTopLevel() && $token->isKeyword($keyword)) {
                $found = $token;
            }
        }

        return $found;
    }

    /**
     * @param list<SqlToken> $tokens
     * @return list<SqlToken>
     */
    public function mergeWhenTokens(array $tokens): array
    {
        $whenTokens = [];
        $caseDepth = 0;
        foreach ($tokens as $index => $token) {
            if (!$token->isTopLevel()) {
                continue;
            }
            if ($token->isKeyword('CASE')) {
                $caseDepth++;
                continue;
            }
            if ($token->isKeyword('END')) {
                if ($caseDepth !== 0) {
                    $caseDepth--;
                    continue;
                }
            }
            if ($caseDepth !== 0 || !$token->isKeyword('WHEN')) {
                continue;
            }
            $next = $tokens[$index + 1] ?? null;
            if ($next === null) {
                continue;
            }
            if ($next->isKeyword('MATCHED')) {
                $whenTokens[] = $token;
                continue;
            }
            if (!$next->isKeyword('NOT')) {
                continue;
            }
            $afterNext = $tokens[$index + 2] ?? null;
            if ($afterNext === null) {
                continue;
            }
            if ($afterNext->isKeyword('MATCHED')) {
                $whenTokens[] = $token;
            }
        }

        return $whenTokens;
    }

    /**
     * @param list<SqlToken> $tokens
     */
    public function keywordIndexOutsideCase(array $tokens, string $keyword): ?int
    {
        $caseDepth = 0;
        foreach ($tokens as $index => $token) {
            if (!$token->isTopLevel()) {
                continue;
            }
            if ($token->isKeyword('CASE')) {
                $caseDepth++;
                continue;
            }
            if ($token->isKeyword('END')) {
                if ($caseDepth !== 0) {
                    $caseDepth--;
                    continue;
                }
            }
            if ($caseDepth === 0 && $token->isKeyword($keyword)) {
                return $index;
            }
        }

        return null;
    }
}
