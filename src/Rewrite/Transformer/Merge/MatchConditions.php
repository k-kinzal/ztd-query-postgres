<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Rewrite\Transformer\Merge;

use ZtdQuery\Platform\Postgres\Sql\Merge\PgSqlMergeMatchKind;
use ZtdQuery\Platform\Postgres\Sql\Merge\PgSqlMergeStatement;

/**
 * Match conditions operations for PostgreSQL merge.
 *
 * @visibility root
 */
final class MatchConditions
{
    /**
     * @return list<string>
     */
    public function effectiveConditions(PgSqlMergeStatement $statement): array
    {
        $priorMatched = [];
        $priorNotMatched = [];
        $effective = [];
        foreach ($statement->clauses as $clause) {
            $prior = $clause->matchKind === PgSqlMergeMatchKind::Matched
                ? $priorMatched
                : $priorNotMatched;
            $condition = $clause->conditionSql ?? 'TRUE';
            $predicate = '(' . $condition . ')';
            if ($prior !== []) {
                $excluded = array_map(
                    static fn (string $previous): string => 'COALESCE((' . $previous . '), FALSE)',
                    $prior,
                );
                $predicate .= ' AND NOT (' . implode(' OR ', $excluded) . ')';
            }
            $effective[] = $predicate;
            if ($clause->matchKind === PgSqlMergeMatchKind::Matched) {
                $priorMatched[] = $condition;
            } else {
                $priorNotMatched[] = $condition;
            }
        }

        return $effective;
    }
}
