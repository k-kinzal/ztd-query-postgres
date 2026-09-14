<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Schema\Reflection\Key;

use ZtdQuery\Schema\Key\PartialUniqueIndex;

/**
 * Carries complete unique-index metadata for one reflected PostgreSQL table.
 *
 * @visibility root
 */
final class IndexDefinitions
{
    /**
     * Retains separately rendered unique constraints and predicate-aware indexes.
     * @param list<string> $sql
     * @param array<string, PartialUniqueIndex> $partialIndexes
     */
    public function __construct(public readonly array $sql, public readonly array $partialIndexes)
    {
    }
}
