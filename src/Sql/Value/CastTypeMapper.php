<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Sql\Value;

use ZtdQuery\Schema\ColumnDeclaration;
use ZtdQuery\Schema\ColumnTypeFamily;

/**
 * Resolves a portable column type to its PostgreSQL CAST target.
 *
 * @visibility root
 */
final class CastTypeMapper
{
    /**
     * Keeps native arrays and domains intact while mapping known scalar families.
     */
    public function map(ColumnDeclaration $type): string
    {
        $nativeType = trim($type->nativeType);
        if (str_ends_with($nativeType, '[]')) {
            return $nativeType;
        }

        if ($type->family === ColumnTypeFamily::UNKNOWN) {
            return $nativeType !== '' ? $nativeType : 'TEXT';
        }

        return match ($type->family) {
            ColumnTypeFamily::INTEGER => NativeCastTarget::integer($nativeType),
            ColumnTypeFamily::FLOAT => 'REAL',
            ColumnTypeFamily::DOUBLE => 'DOUBLE PRECISION',
            ColumnTypeFamily::DECIMAL => NativeCastTarget::decimal($type->nativeType),
            ColumnTypeFamily::STRING => NativeCastTarget::string($type->nativeType),
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
}
