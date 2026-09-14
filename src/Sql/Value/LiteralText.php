<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Sql\Value;

use Stringable;

/**
 * Formats validated PHP scalars as PostgreSQL literal text.
 *
 * @visibility root
 */
final class LiteralText
{
    /**
     * Quotes a validated scalar using PostgreSQL literal escaping.
     */
    public function quoted(bool|int|float|string|Stringable $value): string
    {
        $literal = match (true) {
            is_bool($value) => $value ? '1' : '0',
            is_float($value) => var_export($value, true),
            default => (string) $value,
        };
        return "'" . str_replace("'", "''", $literal) . "'";
    }

    /**
     * Preserves the existing untyped boolean, float and Stringable expression semantics.
     */
    public function untyped(bool|float|Stringable $value): string
    {
        return match (true) {
            is_bool($value) => $value ? 'TRUE' : 'FALSE',
            is_float($value) => var_export($value, true),
            default => (string) $value,
        };
    }
}
