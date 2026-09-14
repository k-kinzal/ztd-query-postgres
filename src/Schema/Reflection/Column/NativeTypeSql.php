<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Schema\Reflection\Column;

/**
 * Native type sql operations for PostgreSQL column.
 *
 * @visibility root
 */
final class NativeTypeSql
{
    /**
     * @template T
     * @param array<string, T> $col
     */
    public function buildTypeSql(string $dataType, string $udtName, array $col): string
    {
        return match ($dataType) {
            'CHARACTER VARYING' => $this->buildVarcharType($col),
            'CHARACTER' => $this->buildCharType($col),
            'BIT', 'BIT VARYING' => $this->buildBitType($dataType, $col),
            'NUMERIC' => $this->buildNumericType($col),
            'TIMESTAMP WITHOUT TIME ZONE' => 'TIMESTAMP',
            'TIMESTAMP WITH TIME ZONE' => 'TIMESTAMPTZ',
            'TIME WITHOUT TIME ZONE' => 'TIME',
            'TIME WITH TIME ZONE' => 'TIMETZ',
            'USER-DEFINED' => $this->resolveUserDefinedType($udtName),
            'ARRAY' => $this->resolveArrayType($udtName),
            default => $dataType,
        };
    }

    /**
     * @template T
     * @param array<string, T> $col
     */
    public function buildVarcharType(array $col): string
    {
        $maxLen = $col['character_maximum_length'] ?? null;
        if (is_int($maxLen) || (is_string($maxLen) && ctype_digit($maxLen))) {
            return "VARCHAR($maxLen)";
        }

        return 'VARCHAR';
    }

    /**
     * @template T
     * @param array<string, T> $col
     */
    public function buildCharType(array $col): string
    {
        $maxLen = $col['character_maximum_length'] ?? null;
        if (is_int($maxLen) || (is_string($maxLen) && ctype_digit($maxLen))) {
            return "CHAR($maxLen)";
        }

        return 'CHAR(1)';
    }

    /**
     * @template T
     * @param array<string, T> $col
     */
    public function buildBitType(string $dataType, array $col): string
    {
        $maxLen = $col['character_maximum_length'] ?? null;
        if (is_int($maxLen) || (is_string($maxLen) && ctype_digit($maxLen))) {
            return "$dataType($maxLen)";
        }

        return $dataType;
    }

    /**
     * @template T
     * @param array<string, T> $col
     */
    public function buildNumericType(array $col): string
    {
        $precision = $col['numeric_precision'] ?? null;
        $scale = $col['numeric_scale'] ?? null;

        if ($precision !== null && $scale !== null) {
            $p = is_int($precision) ? $precision : (is_string($precision) ? (int) $precision : 0);
            $s = is_int($scale) ? $scale : (is_string($scale) ? (int) $scale : 0);

            return "NUMERIC($p,$s)";
        }
        if ($precision !== null) {
            $p = is_int($precision) ? $precision : (is_string($precision) ? (int) $precision : 0);

            return "NUMERIC($p)";
        }

        return 'NUMERIC';
    }

    /**
     * Resolve user defined type.
     */
    public function resolveUserDefinedType(string $udtName): string
    {
        return match ($udtName) {
            'CITEXT' => 'CITEXT',
            'HSTORE' => 'HSTORE',
            'LTREE' => 'LTREE',
            default => $udtName !== '' ? $udtName : 'TEXT',
        };
    }

    /**
     * Resolve array type.
     */
    public function resolveArrayType(string $udtName): string
    {
        if (str_starts_with($udtName, '_')) {
            $baseType = strtoupper(substr($udtName, 1));

            return $baseType . '[]';
        }

        return $udtName . '[]';
    }
}
