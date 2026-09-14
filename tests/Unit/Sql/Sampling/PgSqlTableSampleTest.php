<?php

declare(strict_types=1);

namespace Tests\Unit\Sql\Sampling;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Platform\Postgres\Sql\Sampling\PgSqlTableSample;
use ZtdQuery\Platform\Postgres\Sql\Sampling\PgSqlTableSampleMethod;

#[CoversClass(PgSqlTableSample::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(PgSqlTableSampleMethod::class)]
final class PgSqlTableSampleTest extends TestCase
{
    public function testStoresParsedSampleFields(): void
    {
        $sample = new PgSqlTableSample(
            'data',
            'public.data',
            'AS sampled',
            PgSqlTableSampleMethod::Bernoulli,
            '$1',
            '42.5',
            14,
            78,
        );

        self::assertSame('data', $sample->tableName);
        self::assertSame('public.data', $sample->sourceSql);
        self::assertSame('AS sampled', $sample->aliasSql);
        self::assertSame(PgSqlTableSampleMethod::Bernoulli, $sample->method);
        self::assertSame('$1', $sample->percentageSql);
        self::assertSame('42.5', $sample->seedSql);
        self::assertSame(14, $sample->startOffset);
        self::assertSame(78, $sample->endOffset);
    }











    public function testAcceptsZeroStartOffset(): void
    {
        $sample = new PgSqlTableSample('data', 'data', '', PgSqlTableSampleMethod::System, '10', null, 0, 1);

        self::assertSame(0, $sample->startOffset);
    }
}
