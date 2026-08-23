<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres;

use ZtdQuery\Schema\TablePartitionRelation;

final class PgSqlPartitionPredicateRenderer
{
    /** @param list<string> $siblingPredicates */
    public function render(TablePartitionRelation $relation, array $siblingPredicates): string
    {
        if ($relation->predicate !== null) {
            return $relation->predicate;
        }
        if ($siblingPredicates === []) {
            return 'TRUE';
        }

        $predicates = array_map(
            static fn (string $predicate): string => "($predicate)",
            $siblingPredicates,
        );

        return 'COALESCE(NOT (' . implode(' OR ', $predicates) . '), TRUE)';
    }
}
