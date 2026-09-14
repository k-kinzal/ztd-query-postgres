<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Rewrite\Sampling;

use ZtdQuery\Platform\Postgres\Sql\PgSqlIdentifierQuoter;
use ZtdQuery\Platform\Postgres\Sql\Sampling\PgSqlTableSample;
use ZtdQuery\Platform\Postgres\Sql\Sampling\PgSqlTableSampleMethod;

/**
 * Sample projection operations for PostgreSQL sampling.
 *
 * @visibility root
 */
final class SampleProjection
{
    private readonly PgSqlIdentifierQuoter $quoter;

    /**
     * Supplies the dependencies used by this SampleProjection.
     */
    public function __construct(PgSqlIdentifierQuoter $quoter)
    {
        $this->quoter = $quoter;
    }

    /**
     * @param non-empty-list<string> $columns
     */
    public function replacement(PgSqlTableSample $sample, array $columns, int $index): string
    {
        $sourceAlias = $this->quoter->quote("__ztd_sample_source_$index");
        $parametersAlias = $this->quoter->quote("__ztd_sample_parameters_$index");
        $ordinal = $this->quoter->quote('__ztd_sample_ordinal');
        $percentage = $this->quoter->quote('__ztd_sample_percentage');
        $seed = $this->quoter->quote('__ztd_sample_seed');
        $columnList = implode(', ', array_map($this->quoter->quote(...), $columns));
        $seedSql = $sample->seedSql === null ? 'random()' : "CAST(({$sample->seedSql}) AS DOUBLE PRECISION)";
        $sampleKey = $sample->method === PgSqlTableSampleMethod::System ? '0' : "$sourceAlias.$ordinal";
        $randomValue = "((('x' || SUBSTRING(MD5(CAST($sampleKey AS TEXT) || ':' || "
            . "CAST($parametersAlias.$seed AS TEXT)), 1, 8))::BIT(32)::BIGINT)::DOUBLE PRECISION "
            . '/ 4294967296.0) * 100';
        $valid = "$parametersAlias.$percentage IS NOT NULL"
            . " AND $parametersAlias.$percentage >= 0"
            . " AND $parametersAlias.$percentage <= 100"
            . " AND $parametersAlias.$seed IS NOT NULL";
        $invalid = "CAST('invalid sample argument: ' || COALESCE(CAST($parametersAlias.$percentage AS TEXT), 'NULL') AS BOOLEAN)";
        $predicate = "CASE WHEN $valid THEN $randomValue < $parametersAlias.$percentage ELSE $invalid END";
        $alias = $sample->aliasSql !== ''
            ? ' ' . $sample->aliasSql
            : ' AS ' . $this->quoter->quote($sample->tableName);

        return '(SELECT ' . $columnList
            . ' FROM (SELECT ' . $columnList . ", ROW_NUMBER() OVER () AS $ordinal"
            . " FROM {$sample->sourceSql}) AS $sourceAlias"
            . " CROSS JOIN (SELECT CAST(({$sample->percentageSql}) AS DOUBLE PRECISION) AS $percentage, "
            . "$seedSql AS $seed) AS $parametersAlias"
            . " WHERE $predicate)$alias";
    }
}
