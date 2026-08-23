<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres;

use ZtdQuery\Platform\ResultColumnTypeResolver;
use ZtdQuery\Schema\ColumnType;

final class PgSqlPdoResultColumnTypeResolver implements ResultColumnTypeResolver
{
    public function resolve(array $metadata): ColumnType
    {
        $nativeType = $metadata['native_type'] ?? '';
        if (!is_string($nativeType)) {
            return (new PgSqlColumnTypeMapper())->map('');
        }
        if (str_starts_with($nativeType, '_')) {
            $nativeType = substr($nativeType, 1) . '[]';
        }

        $typeModifier = $metadata['precision'] ?? null;
        if (is_int($typeModifier) && $typeModifier > 4 && !str_contains($nativeType, '(')) {
            $length = $typeModifier - 4;
            $nativeType = match (strtoupper($nativeType)) {
                'VARCHAR' => "VARCHAR($length)",
                'BPCHAR' => "CHAR($length)",
                default => $nativeType,
            };
        }

        return (new PgSqlColumnTypeMapper())->map($nativeType);
    }
}
