<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Rewrite\Transformer\Insert;

use ZtdQuery\Platform\Postgres\Rewrite\Upsert\PgSqlNativeUpsertProjector;
use ZtdQuery\Platform\Postgres\Sql\PgSqlParser;
use ZtdQuery\Schema\Key\CandidateKeySet;
use ZtdQuery\Schema\Key\PartialUniqueIndex;

/**
 * Upsert projection operations for PostgreSQL insert.
 *
 * @visibility root
 */
final class UpsertProjection
{
    private readonly PgSqlParser $parser;
    private readonly PgSqlNativeUpsertProjector $upsertProjector;

    /**
     * Supplies the dependencies used by this UpsertProjection.
     */
    public function __construct(PgSqlParser $parser, PgSqlNativeUpsertProjector $upsertProjector)
    {
        $this->parser = $parser;
        $this->upsertProjector = $upsertProjector;
    }

    /**
     * @param list<string> $tableColumns
     * @param array<string, array{
     *     candidateKeys?: array<string, array<int, string>>,
     *     partialUniqueIndexes?: array<string, PartialUniqueIndex>
     * }> $tables
     */
    public function projectUpsert(
        string $sql,
        string $selectSql,
        string $tableName,
        array $tableColumns,
        array $tables,
    ): string {
        $conflict = $this->parser->extractOnConflictUpdateColumns($sql);
        $candidateKeys = new CandidateKeySet(
            isset($tables[$tableName]['candidateKeys']) ? $tables[$tableName]['candidateKeys'] : [],
        );
        $conflictPredicate = null;
        $target = $this->parser->extractOnConflictTarget($sql);
        if ($target !== null) {
            $resolved = $target->resolve(
                $candidateKeys,
                isset($tables[$tableName]['partialUniqueIndexes'])
                    ? $tables[$tableName]['partialUniqueIndexes']
                    : [],
                $sql,
            );
            $candidateKeys = $resolved['keys'];
            $conflictPredicate = $resolved['predicate'];
        }

        return $this->upsertProjector->project(
            $selectSql,
            $tableName,
            $tableColumns,
            $candidateKeys->keys(),
            $conflict['values'],
            $this->parser->extractOnConflictUpdateWhere($sql),
            $conflictPredicate,
        );
    }
}
