<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Sql\Merge;

use ZtdQuery\Exception\UnsupportedSqlException;
use ZtdQuery\Sql\SqlToken;

/**
 * Resolves the target, join and branch spans of a MERGE statement.
 *
 * @visibility root
 */
final class StatementParts
{
    /**
     * Resolves the target, alias and USING delimiter after MERGE modifiers.
     * @param list<SqlToken> $tokens
     * @return array{name: string, sql: string, alias: string, using: SqlToken}
     * @throws UnsupportedSqlException
     */
    public function target(string $sql, string $statementSql, array $tokens): array
    {
        $index = 0;
        $first = $tokens[$index] ?? null;
        if ($first === null) {
            throw new UnsupportedSqlException($sql, 'Malformed MERGE statement');
        }
        if (!$first->isKeyword('MERGE')) {
            throw new UnsupportedSqlException($sql, 'Malformed MERGE statement');
        }
        $index++;
        $next = $tokens[$index] ?? null;
        if ($next !== null && $next->isKeyword('INTO')) {
            $index++;
            $next = $tokens[$index] ?? null;
        }
        if ($next !== null && $next->isKeyword('ONLY')) {
            $index++;
        }

        $target = (new RelationTarget())->relationAt($statementSql, $tokens, $index);
        if ($target === null) {
            throw new UnsupportedSqlException($sql, 'Cannot resolve MERGE target');
        }
        $index = $target['next'];
        if ((new RelationTarget())->isSymbol($tokens[$index] ?? null, '*')) {
            $target['last'] = $tokens[$index];
            $index++;
        }

        $usingIndex = (new BranchTokens())->keywordIndexAfter($tokens, 'USING', $target['last']);
        if ($usingIndex === null) {
            throw new UnsupportedSqlException($sql, 'MERGE requires USING');
        }
        $targetAlias = (new RelationTarget())->targetAlias($sql, $tokens, $index, $usingIndex, $target['name']);

        return ['name' => $target['name'], 'sql' => $target['sql'], 'alias' => $targetAlias, 'using' => $tokens[$usingIndex]];
    }

    /**
     * Separates the source relation and join condition before the first branch.
     * @param list<SqlToken> $tokens
     * @return array{source: string, condition: string, when: list<SqlToken>}
     * @throws UnsupportedSqlException
     */
    public function join(string $sql, string $statementSql, array $tokens, SqlToken $usingToken): array
    {
        $whenTokens = (new BranchTokens())->mergeWhenTokens($tokens);
        $firstWhen = $whenTokens[0] ?? null;
        if ($firstWhen === null) {
            throw new UnsupportedSqlException($sql, 'MERGE requires a WHEN clause');
        }
        $onToken = (new BranchTokens())->lastKeywordBetween($tokens, 'ON', $usingToken, $firstWhen);
        if ($onToken === null) {
            throw new UnsupportedSqlException($sql, 'MERGE requires an ON condition');
        }

        $sourceSql = trim(substr(
            $statementSql,
            $usingToken->endOffset(),
            $onToken->offset - $usingToken->endOffset(),
        ));
        $joinConditionSql = trim(substr(
            $statementSql,
            $onToken->endOffset(),
            $firstWhen->offset - $onToken->endOffset(),
        ));
        if ($sourceSql === '') {
            throw new UnsupportedSqlException($sql, 'MERGE requires a source and join condition');
        }
        if ($joinConditionSql === '') {
            throw new UnsupportedSqlException($sql, 'MERGE requires a source and join condition');
        }

        return ['source' => $sourceSql, 'condition' => $joinConditionSql, 'when' => $whenTokens];
    }

    /**
     * Parses every WHEN branch using its exact source offsets.
     * @param list<SqlToken> $whenTokens
     * @return non-empty-list<PgSqlMergeClause>
     * @throws UnsupportedSqlException
     */
    public function clauses(string $sql, string $statementSql, array $whenTokens): array
    {
        $clauses = [];
        foreach ($whenTokens as $clauseIndex => $whenToken) {
            $end = isset($whenTokens[$clauseIndex + 1])
                ? $whenTokens[$clauseIndex + 1]->offset
                : strlen($statementSql);
            $clauseSql = substr($statementSql, $whenToken->offset, $end - $whenToken->offset);
            $clauses[] = (new ActionClause())->parseClause($sql, $clauseSql);
        }
        if ($clauses === []) {
            throw new UnsupportedSqlException($sql, 'MERGE requires a WHEN clause');
        }

        return $clauses;
    }
}
