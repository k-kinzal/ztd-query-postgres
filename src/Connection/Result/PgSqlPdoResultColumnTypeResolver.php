<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Connection\Result;

use ZtdQuery\Platform\Postgres\Schema\PgSqlColumnTypeMapper;
use ZtdQuery\Platform\ResultColumnTypeResolver;
use ZtdQuery\Schema\ColumnDeclaration;

/**
 * Pdo result column type resolver for PostgreSQL queries.
 */
final class PgSqlPdoResultColumnTypeResolver implements ResultColumnTypeResolver
{
    /**
     * Maps PostgreSQL result metadata to the portable column type contract.
     */
    public function resolve(array $metadata): ColumnDeclaration
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
