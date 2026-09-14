<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Rewrite\Transformer\Insert;

/**
 * Ordered expressions operations for PostgreSQL insert.
 *
 * @visibility root
 */
final class OrderedExpressions
{
    /**
     * @template T
     * @param array<array-key, T> $values
     * @return list<T>
     */
    public static function orderedValues(array $values): array
    {
        $ordered = [];
        foreach ($values as $value) {
            $ordered[] = $value;
        }

        return $ordered;
    }
}
