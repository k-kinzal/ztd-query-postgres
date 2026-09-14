<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Rewrite\Upsert;

use ZtdQuery\Platform\IdentifierQuoter;

/**
 * Conflict predicate operations for PostgreSQL upsert.
 *
 * @visibility root
 */
final class ConflictPredicate
{
    private readonly IdentifierQuoter $quoter;

    /**
     * Supplies the dependencies used by this ConflictPredicate.
     */
    public function __construct(IdentifierQuoter $quoter)
    {
        $this->quoter = $quoter;
    }

    /**
     * @param array<string, array<int, string>> $candidateKeys
     */
    public function conflictPredicate(array $candidateKeys, string $existingAlias, string $incomingAlias): string
    {
        $keys = [];
        foreach ($candidateKeys as $columns) {
            if ($columns === []) {
                continue;
            }
            $comparisons = [];
            foreach ($columns as $column) {
                $quoted = $this->quoter->quote($column);
                $comparisons[] = "$existingAlias.$quoted = $incomingAlias.$quoted";
            }
            $keys[] = '(' . implode(' AND ', $comparisons) . ')';
        }

        return $keys === [] ? 'FALSE' : '(' . implode(' OR ', $keys) . ')';
    }

    /**
     * Qualified.
     */
    public function qualified(string $alias, string $column): string
    {
        return $this->quoter->quote($alias) . '.' . $this->quoter->quote($column);
    }
    /**
     * Applies a partial-index predicate to both existing and incoming candidate rows.
     * @param list<string> $tableColumns
     * @param non-empty-list<string> $incomingNamespaces
     */
    public function withPredicate(string $conflict, string $conflictPredicate, string $tableName, array $tableColumns, array $incomingNamespaces): string
    {
        $existingPredicate = (new ExpressionBinder($incomingNamespaces, $this->quoter))->bindExpression(
            $conflictPredicate,
            $tableName,
            $tableColumns,
            '__ztd_existing',
        );
        $incomingPredicate = (new ExpressionBinder($incomingNamespaces, $this->quoter))->bindExpression(
            $conflictPredicate,
            $tableName,
            $tableColumns,
            '__ztd_incoming',
        );
        $conflict = "($conflict AND ($existingPredicate) AND ($incomingPredicate))";
        return $conflict;
    }
}
