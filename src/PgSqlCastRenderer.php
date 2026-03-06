<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres;

use ZtdQuery\Platform\CastRenderer;
use ZtdQuery\Schema\ColumnType;
use ZtdQuery\Schema\ColumnTypeFamily;

/**
 * PostgreSQL CAST expression renderer.
 *
 * Maps ColumnType to PostgreSQL-specific CAST syntax.
 * Uses standard CAST() syntax (not :: shorthand) for maximum compatibility.
 */
final class PgSqlCastRenderer implements CastRenderer
{
    /**
     * {@inheritDoc}
     */
    public function renderCast(string $expression, ColumnType $type): string
    {
        $castType = $this->mapToCastType($type);

        return "CAST($expression AS $castType)";
    }

    /**
     * {@inheritDoc}
     */
    public function renderNullCast(ColumnType $type): string
    {
        $castType = $this->mapToCastType($type);

        return "CAST(NULL AS $castType)";
    }

    private function mapToCastType(ColumnType $type): string
    {
        if ($type->family === ColumnTypeFamily::UNKNOWN) {
            return $type->nativeType !== '' ? $type->nativeType : 'TEXT';
        }

        return match ($type->family) {
            ColumnTypeFamily::INTEGER => 'INTEGER',
            ColumnTypeFamily::FLOAT => 'REAL',
            ColumnTypeFamily::DOUBLE => 'DOUBLE PRECISION',
            ColumnTypeFamily::DECIMAL => $this->extractDecimalType($type->nativeType),
            ColumnTypeFamily::STRING => $this->extractStringType($type->nativeType),
            ColumnTypeFamily::TEXT => 'TEXT',
            ColumnTypeFamily::BOOLEAN => 'BOOLEAN',
            ColumnTypeFamily::DATE => 'DATE',
            ColumnTypeFamily::TIME => 'TIME',
            ColumnTypeFamily::DATETIME => 'TIMESTAMP',
            ColumnTypeFamily::TIMESTAMP => 'TIMESTAMP',
            ColumnTypeFamily::BINARY => 'BYTEA',
            ColumnTypeFamily::JSON => 'JSONB',
        };
    }

    private function extractDecimalType(string $nativeType): string
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

    private function extractStringType(string $nativeType): string
    {
        $upper = strtoupper($nativeType);
        if (preg_match('/VARCHAR\((\d+)\)/', $upper, $matches) === 1) {
            return "VARCHAR({$matches[1]})";
        }

        return 'TEXT';
    }
}
