<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Sql\Conflict;

use InvalidArgumentException;
use ZtdQuery\Exception\UnsupportedSqlException;
use ZtdQuery\Schema\Key\CandidateKeySet;
use ZtdQuery\Schema\Key\PartialUniqueIndex;

/**
 * Structured ON CONFLICT arbiter target.
 *
 * @visibility public
 * @example Represent a column-based conflict target
 *     $target = new \ZtdQuery\Platform\Postgres\Sql\Conflict\PgSqlConflictTarget(true, ['id']);
 *     $target->columns // => ['id']
 *     $target->constraint // => null
 */
final class PgSqlConflictTarget
{
    /**
     * @param array<int, string> $columns
     * @throws InvalidArgumentException
     */
    public function __construct(
        public readonly bool $specified,
        public readonly array $columns = [],
        public readonly ?string $predicate = null,
        public readonly ?string $constraint = null,
    ) {
        if ($constraint !== null && $columns !== []) {
            throw new InvalidArgumentException('A conflict target cannot use columns and a constraint together.');
        }
    }

    /**
     * @param array<string, PartialUniqueIndex> $partialIndexes
     * @return array{keys: CandidateKeySet, predicate: string|null}
     * @throws UnsupportedSqlException
     * @visibility public
     * @example Resolve a conflict target against the catalog's candidate keys
     *     $target = new \ZtdQuery\Platform\Postgres\Sql\Conflict\PgSqlConflictTarget(true, ['id']);
     *     $keys = new \ZtdQuery\Schema\Key\CandidateKeySet(['users_pkey' => ['id'], 'users_email_key' => ['email']]);
     *     $resolved = $target->resolve($keys, [], 'INSERT INTO users VALUES (1) ON CONFLICT (id) DO NOTHING');
     *     $resolved['keys']->keys() // => ['users_pkey' => ['id']]
     *     $resolved['predicate'] // => null
     */
    public function resolve(CandidateKeySet $candidateKeys, array $partialIndexes, string $sql): array
    {
        if (!$this->specified) {
            return ['keys' => $candidateKeys, 'predicate' => null];
        }

        if ($this->constraint !== null) {
            foreach ($candidateKeys->keys() as $name => $columns) {
                if (strcasecmp($name, $this->constraint) === 0) {
                    return ['keys' => new CandidateKeySet([$name => $columns]), 'predicate' => null];
                }
            }

            throw new UnsupportedSqlException($sql, 'Cannot resolve ON CONFLICT constraint');
        }

        $targetColumns = ColumnSet::normalizedColumns($this->columns);
        $fullMatches = [];
        foreach ($candidateKeys->keys() as $name => $columns) {
            if (ColumnSet::normalizedColumns($columns) === $targetColumns) {
                $fullMatches[$name] = $columns;
            }
        }
        if ($fullMatches !== []) {
            return ['keys' => new CandidateKeySet($fullMatches), 'predicate' => null];
        }

        $partialMatches = [];
        foreach ($partialIndexes as $name => $index) {
            if (ColumnSet::normalizedColumns($index->columns) === $targetColumns) {
                $partialMatches[$name] = $index->columns;
            }
        }
        if ($this->predicate === null || count($partialMatches) !== 1) {
            throw new UnsupportedSqlException($sql, 'Cannot resolve ON CONFLICT partial index');
        }

        return ['keys' => new CandidateKeySet($partialMatches), 'predicate' => $this->predicate];
    }
}
