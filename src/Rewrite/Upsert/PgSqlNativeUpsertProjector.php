<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Rewrite\Upsert;

use ZtdQuery\Platform\IdentifierQuoter;
use ZtdQuery\Platform\Postgres\Sql\PgSqlIdentifierQuoter;
use ZtdQuery\Shadow\Mutation\UpsertMutationRow;

/**
 * Native upsert projector for PostgreSQL queries.
 */
final class PgSqlNativeUpsertProjector
{
    private const INCOMING_ALIAS = '__ztd_incoming';

    private const EXISTING_ALIAS = '__ztd_existing';

    private readonly IdentifierQuoter $quoter;

    /**
     * @var non-empty-list<string>
     */
    private readonly array $incomingNamespaces;

    /**
     * Initializes the collaborators and state used by this native upsert projector.
     */
    public function __construct()
    {
        $this->quoter = new PgSqlIdentifierQuoter();
        $this->incomingNamespaces = ['EXCLUDED'];
    }

    /**
     * @param list<string> $tableColumns
     * @param array<string, array<int, string>> $candidateKeys
     * @param array<string, string> $assignments
     */
    public function project(
        string $incomingSql,
        string $tableName,
        array $tableColumns,
        array $candidateKeys,
        array $assignments,
        ?string $predicate = null,
        ?string $conflictPredicate = null,
    ): string {
        if ($assignments === [] || $candidateKeys === []) {
            return $incomingSql;
        }

        $incomingAlias = $this->quoter->quote(self::INCOMING_ALIAS);
        $existingAlias = $this->quoter->quote(self::EXISTING_ALIAS);
        $table = $this->quoter->quote($tableName);
        $conflict = (new ConflictPredicate($this->quoter))->conflictPredicate($candidateKeys, $existingAlias, $incomingAlias);
        if ($conflictPredicate !== null) {
            $conflict = (new ConflictPredicate($this->quoter))->withPredicate($conflict, $conflictPredicate, $tableName, $tableColumns, $this->incomingNamespaces);
        }
        $selects = [];
        foreach ($tableColumns as $column) {
            $quoted = $this->quoter->quote($column);
            $selects[] = "$incomingAlias.$quoted AS $quoted";
        }

        $codec = new UpsertMutationRow();
        foreach (array_values($assignments) as $index => $expression) {
            $evaluated = (new ExpressionBinder($this->incomingNamespaces, $this->quoter))->bindExpression($expression, $tableName, $tableColumns);
            $metadata = $this->quoter->quote($codec->valueColumn($index));
            $selects[] = "(SELECT $evaluated FROM $table AS $existingAlias WHERE $conflict LIMIT 1) AS $metadata";
        }
        if ($predicate !== null) {
            $evaluated = (new ExpressionBinder($this->incomingNamespaces, $this->quoter))->bindExpression($predicate, $tableName, $tableColumns);
            $metadata = $this->quoter->quote($codec->predicateColumn());
            $selects[] = "(SELECT $evaluated FROM $table AS $existingAlias WHERE $conflict LIMIT 1) AS $metadata";
        }

        return 'SELECT ' . implode(', ', $selects) . " FROM ($incomingSql) AS $incomingAlias";
    }
}
