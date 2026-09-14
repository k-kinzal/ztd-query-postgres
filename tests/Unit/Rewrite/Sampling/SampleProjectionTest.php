<?php

declare(strict_types=1);

namespace Tests\Unit\Rewrite\Sampling;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Rewrite\Sampling\SampleProjection::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlIdentifierQuoter::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Sampling\PgSqlTableSample::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Sampling\PgSqlTableSampleMethod::class)]
final class SampleProjectionTest extends TestCase
{
    public function testReplacement(): void
    {
        $sample = new \ZtdQuery\Platform\Postgres\Sql\Sampling\PgSqlTableSample('users', 'users', 'u', \ZtdQuery\Platform\Postgres\Sql\Sampling\PgSqlTableSampleMethod::System, '10', '7', 0, 50);

        self::assertSame('(SELECT "id", "name" FROM (SELECT "id", "name", ROW_NUMBER() OVER () AS "__ztd_sample_ordinal" FROM users) AS "__ztd_sample_source_1" CROSS JOIN (SELECT CAST((10) AS DOUBLE PRECISION) AS "__ztd_sample_percentage", CAST((7) AS DOUBLE PRECISION) AS "__ztd_sample_seed") AS "__ztd_sample_parameters_1" WHERE CASE WHEN "__ztd_sample_parameters_1"."__ztd_sample_percentage" IS NOT NULL AND "__ztd_sample_parameters_1"."__ztd_sample_percentage" >= 0 AND "__ztd_sample_parameters_1"."__ztd_sample_percentage" <= 100 AND "__ztd_sample_parameters_1"."__ztd_sample_seed" IS NOT NULL THEN (((\'x\' || SUBSTRING(MD5(CAST(0 AS TEXT) || \':\' || CAST("__ztd_sample_parameters_1"."__ztd_sample_seed" AS TEXT)), 1, 8))::BIT(32)::BIGINT)::DOUBLE PRECISION / 4294967296.0) * 100 < "__ztd_sample_parameters_1"."__ztd_sample_percentage" ELSE CAST(\'invalid sample argument: \' || COALESCE(CAST("__ztd_sample_parameters_1"."__ztd_sample_percentage" AS TEXT), \'NULL\') AS BOOLEAN) END) u', (new \ZtdQuery\Platform\Postgres\Rewrite\Sampling\SampleProjection(new \ZtdQuery\Platform\Postgres\Sql\PgSqlIdentifierQuoter()))->replacement($sample, ['id', 'name'], 1));
    }
}
