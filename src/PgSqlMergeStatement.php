<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres;

final class PgSqlMergeStatement
{
    /** @param non-empty-list<PgSqlMergeClause> $clauses */
    public function __construct(
        public readonly string $targetTable,
        public readonly string $targetSql,
        public readonly string $targetAlias,
        public readonly string $sourceSql,
        public readonly string $joinConditionSql,
        public readonly array $clauses,
    ) {
    }
}
