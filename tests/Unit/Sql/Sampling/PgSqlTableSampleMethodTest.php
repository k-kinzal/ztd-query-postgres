<?php

declare(strict_types=1);

namespace Tests\Unit\Sql\Sampling;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Platform\Postgres\Sql\Sampling\PgSqlTableSampleMethod;

#[CoversClass(PgSqlTableSampleMethod::class)]
final class PgSqlTableSampleMethodTest extends TestCase
{
    public function testMethodsUsePostgreSqlKeywords(): void
    {
        self::assertSame('BERNOULLI', PgSqlTableSampleMethod::Bernoulli->value);
        self::assertSame('SYSTEM', PgSqlTableSampleMethod::System->value);
    }
}
