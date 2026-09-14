<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Sql\Value;

/**
 * Normalizes parameterized PostgreSQL CAST target names.
 *
 * @visibility root
 */
final class NativeCastTarget
{
    /**
     * Maps integer aliases while retaining small and big integer widths.
     */
    public static function integer(string $nativeType): string
    {
        $baseType = strtoupper((string) preg_replace('/\(.*\)/', '', $nativeType));

        return match ($baseType) {
            'INT2', 'SMALLINT', 'SMALLSERIAL' => 'SMALLINT',
            'INT8', 'BIGINT', 'BIGSERIAL' => 'BIGINT',
            default => 'INTEGER',
        };
    }

    /**
     * Preserves decimal precision and scale in a NUMERIC target.
     */
    public static function decimal(string $nativeType): string
    {
        $upper = strtoupper($nativeType);
        if (preg_match('/(?:DECIMAL|NUMERIC)\((\d+),(\d+)\)/', $upper, $matches) === 1) {
            return "NUMERIC({$matches[1]},{$matches[2]})";
        }
        if (preg_match('/(?:DECIMAL|NUMERIC)\((\d+)\)/', $upper, $matches) === 1) {
            return "NUMERIC({$matches[1]},0)";
        }

        return 'NUMERIC';
    }

    /**
     * Preserves VARCHAR bounds and falls back to TEXT for other strings.
     */
    public static function string(string $nativeType): string
    {
        $upper = strtoupper($nativeType);
        if (preg_match('/VARCHAR\((\d+)\)/', $upper, $matches) === 1) {
            return "VARCHAR({$matches[1]})";
        }

        if ($upper === 'VARCHAR' || $upper === 'CHARACTER VARYING') {
            return 'VARCHAR';
        }

        return 'TEXT';
    }
}
