<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Rewrite\Cte;

use ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile;
use ZtdQuery\Platform\Postgres\Sql\Relation\PgSqlSelectRelationParser;
use ZtdQuery\Sql\SqlTokenStream;

/**
 * Cte shadow composer for PostgreSQL queries.
 */
final class PgSqlCteShadowComposer
{
    /**
     * @param array<string, string> $tableCtes
     */
    public function compose(string $sql, array $tableCtes): string
    {
        $requiredCtes = (new ShadowDependencies())->required($sql, $tableCtes);
        $ctes = $requiredCtes;
        $shadowedTables = array_keys($requiredCtes);

        if ($ctes === []) {
            return $sql;
        }

        $sql = (new PgSqlSelectRelationParser())->unqualify($sql, $shadowedTables);
        $tokens = SqlTokenStream::tokenize($sql, PgSqlLexerProfile::create())->significantTokens();
        $with = $tokens[0] ?? null;
        if ($with === null || !$with->isKeyword('WITH')) {
            return 'WITH ' . implode(",\n", $ctes) . "\n" . $sql;
        }

        $insertionToken = $with;
        $next = $tokens[1] ?? null;
        if ($next !== null && $next->isTopLevel() && $next->isKeyword('RECURSIVE')) {
            $insertionToken = $next;
        }

        return substr_replace(
            $sql,
            ' ' . implode(",\n", $ctes) . ",\n",
            $insertionToken->endOffset(),
            0,
        );
    }

    /**
     * @return list<string>
     */
    public function declaredCteNames(string $sql): array
    {
        return (new \ZtdQuery\Platform\Postgres\Sql\Cte\HeaderParser())->parseHeader($sql)['names'];
    }

    /**
     * Carries a statement's existing WITH clause into the rewritten SQL.
     */
    public function carryPrefix(string $originalSql, string $rewrittenStatement): string
    {
        $header = (new \ZtdQuery\Platform\Postgres\Sql\Cte\HeaderParser())->parseHeader($originalSql);
        if ($header['statementOffset'] === null) {
            return $rewrittenStatement;
        }

        $prefix = rtrim(substr($originalSql, 0, $header['statementOffset']));

        $rewrittenTokens = SqlTokenStream::tokenize($rewrittenStatement, PgSqlLexerProfile::create())->significantTokens();
        $rewrittenWith = $rewrittenTokens[0] ?? null;
        if ($rewrittenWith !== null && $rewrittenWith->isKeyword('WITH')) {
            $rewrittenHeader = (new \ZtdQuery\Platform\Postgres\Sql\Cte\HeaderParser())->parseHeader($rewrittenStatement);
            $rewrittenStatementOffset = $rewrittenHeader['statementOffset'];
            if ($rewrittenStatementOffset === null) {
                return $prefix . "\n" . $rewrittenStatement;
            }

            $contentToken = $rewrittenWith;
            $rewrittenNext = $rewrittenTokens[1] ?? null;
            if ($rewrittenNext !== null && $rewrittenNext->isKeyword('RECURSIVE')) {
                $contentToken = $rewrittenNext;
            }

            $rewrittenBody = trim(substr(
                $rewrittenStatement,
                $contentToken->endOffset(),
                $rewrittenStatementOffset - $contentToken->endOffset(),
            ));
            $rewrittenTail = substr($rewrittenStatement, $rewrittenStatementOffset);
            if ((new \ZtdQuery\Platform\Postgres\Sql\Cte\IdentifierReferences())->referencesAnyIdentifier($rewrittenBody, $header['names'])) {
                return $prefix . ",\n" . $rewrittenBody . "\n" . $rewrittenTail;
            }

            return (new PrefixMerge())->prependRewritten($originalSql, $header['statementOffset'], $rewrittenBody, $rewrittenTail);
        }

        return $prefix . "\n" . $rewrittenStatement;
    }

    /**
     * Returns the statement body after its WITH declarations.
     */
    public function statementSql(string $sql): string
    {
        return (new \ZtdQuery\Platform\Postgres\Sql\Cte\HeaderParser())->statementSql($sql);
    }
}
