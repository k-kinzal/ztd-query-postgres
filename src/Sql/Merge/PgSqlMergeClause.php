<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Sql\Merge;

/**
 * Describes one MERGE branch, its predicate, and its row action.
 */
final class PgSqlMergeClause
{
    /**
     * @param array<string, string> $assignments
     * @param list<string> $insertColumns
     * @param list<string> $insertValues
     */
    public function __construct(
        public readonly PgSqlMergeMatchKind $matchKind,
        public readonly ?string $conditionSql,
        public readonly PgSqlMergeActionKind $actionKind,
        public readonly array $assignments = [],
        public readonly array $insertColumns = [],
        public readonly array $insertValues = [],
    ) {
    }
}
