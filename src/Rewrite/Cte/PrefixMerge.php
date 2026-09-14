<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Rewrite\Cte;

use ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile;
use ZtdQuery\Sql\SqlTokenStream;

/**
 * Merges independent generated and original WITH declarations in dependency order.
 *
 * @visibility root
 */
final class PrefixMerge
{
    /**
     * Prepends independent rewritten CTEs while preserving the original recursive marker.
     */
    public function prependRewritten(string $originalSql, int $statementOffset, string $rewrittenBody, string $rewrittenTail): string
    {
        $originalTokens = SqlTokenStream::tokenize($originalSql, PgSqlLexerProfile::create())->significantTokens();
        $originalWith = $originalTokens[0];
        $originalContentToken = $originalWith;
        $recursive = false;
        $originalNext = $originalTokens[1] ?? null;
        if ($originalNext !== null && $originalNext->isKeyword('RECURSIVE')) {
            $originalContentToken = $originalNext;
            $recursive = true;
        }
        $originalBody = trim(substr(
            $originalSql,
            $originalContentToken->endOffset(),
            $statementOffset - $originalContentToken->endOffset(),
        ));
        $leading = substr($originalSql, 0, $originalWith->offset);

        return $leading
            . 'WITH '
            . ($recursive ? 'RECURSIVE ' : '')
            . $rewrittenBody
            . ",\n"
            . $originalBody
            . "\n"
            . $rewrittenTail;
    }
}
