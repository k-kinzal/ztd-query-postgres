<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres;

use ZtdQuery\Exception\UnsupportedSqlException;

final class PgSqlTableSampleRewriter
{
    private PgSqlTableSampleParser $parser;
    private PgSqlIdentifierQuoter $quoter;

    public function __construct()
    {
        $this->parser = new PgSqlTableSampleParser();
        $this->quoter = new PgSqlIdentifierQuoter();
    }

    /**
     * @param array<string, array<string, mixed>> $tables
     */
    public function rewrite(string $sql, array $tables): string
    {
        $samples = $this->parser->parse($sql);
        usort($samples, static fn (PgSqlTableSample $left, PgSqlTableSample $right): int => $right->startOffset <=> $left->startOffset);

        foreach ($samples as $index => $sample) {
            $columns = $this->columns($sample->tableName, $tables);
            if ($columns === []) {
                throw new UnsupportedSqlException(
                    $sql,
                    "Cannot determine columns for TABLESAMPLE source '{$sample->tableName}'",
                );
            }
            $replacement = $this->replacement($sample, $columns, $index);
            $sql = substr_replace(
                $sql,
                $replacement,
                $sample->startOffset,
                $sample->endOffset - $sample->startOffset,
            );
        }

        return $sql;
    }

    /**
     * @param array<string, array<string, mixed>> $tables
     * @return list<string>
     */
    private function columns(string $tableName, array $tables): array
    {
        foreach ($tables as $candidate => $context) {
            if (strcasecmp($candidate, $tableName) !== 0) {
                continue;
            }
            $columns = $context['columns'] ?? null;
            if (!is_array($columns)) {
                return [];
            }

            $names = [];
            foreach ($columns as $column) {
                if (is_string($column)) {
                    $names[] = $column;
                }
            }

            return $names;
        }

        return [];
    }

    /** @param non-empty-list<string> $columns */
    private function replacement(PgSqlTableSample $sample, array $columns, int $index): string
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
