<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Schema\Reflection\Column;

use ZtdQuery\Platform\Postgres\Sql\PgSqlIdentifierQuoter;

/**
 * Column definition sql operations for PostgreSQL column.
 *
 * @visibility root
 */
final class ColumnDefinitionSql
{
    /**
     * @template T
     * @param array<string, T> $col
     */
    public function buildColumnDefinition(array $col): string
    {
        $columnName = isset($col['column_name']) && is_string($col['column_name']) ? $col['column_name'] : '';
        $name = '"' . $columnName . '"';
        $dataType = strtoupper(isset($col['data_type']) && is_string($col['data_type']) ? $col['data_type'] : 'TEXT');
        $udtName = strtoupper(isset($col['udt_name']) && is_string($col['udt_name']) ? $col['udt_name'] : '');

        $typeSql = $this->domainTypeSql($col) ?? (new NativeTypeSql())->buildTypeSql($dataType, $udtName, $col);

        $def = "$name $typeSql";

        $isNullable = $col['is_nullable'] ?? 'YES';
        if ($isNullable === 'NO') {
            $def .= ' NOT NULL';
        }

        $def .= $this->generationClause($col);

        return $def;
    }

    /**
     * @template T
     * @param array<string, T> $col
     */
    public function domainTypeSql(array $col): ?string
    {
        $domainName = $col['domain_name'] ?? null;
        if (!is_string($domainName)) {
            return null;
        }
        if ($domainName === '') {
            return null;
        }

        $quoter = new PgSqlIdentifierQuoter();
        $domainSchema = $col['domain_schema'] ?? null;
        if (!is_string($domainSchema)) {
            return $quoter->quote($domainName);
        }
        if ($domainSchema === '') {
            return $quoter->quote($domainName);
        }

        return $quoter->quote($domainSchema) . '.' . $quoter->quote($domainName);
    }
    /**
     * Renders stored expressions, identities or defaults in their catalog precedence.
     * @template T
     * @param array<string, T> $col
     */
    public function generationClause(array $col): string
    {
        $def = '';
        $generationExpression = $col['generation_expression'] ?? null;
        if (($col['is_generated'] ?? 'NEVER') === 'ALWAYS'
            && is_string($generationExpression)
            && trim($generationExpression) !== ''
        ) {
            $def .= ' GENERATED ALWAYS AS (' . $generationExpression . ') STORED';
        } elseif (($col['is_identity'] ?? 'NO') === 'YES') {
            $identityGeneration = ($col['identity_generation'] ?? 'BY DEFAULT') === 'ALWAYS'
                ? 'ALWAYS'
                : 'BY DEFAULT';
            $def .= " GENERATED $identityGeneration AS IDENTITY";
        } else {
            $default = $col['column_default'] ?? null;
            if (is_string($default) && $default !== '') {
                $def .= ' DEFAULT ' . $default;
            }
        }

        return $def;
    }
}
