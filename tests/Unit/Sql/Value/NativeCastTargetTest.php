<?php

declare(strict_types=1);

namespace Tests\Unit\Sql\Value;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Sql\Value\NativeCastTarget::class)]
final class NativeCastTargetTest extends TestCase
{
    public function testIntegerPreservesWidths(): void
    {
        self::assertSame('SMALLINT', \ZtdQuery\Platform\Postgres\Sql\Value\NativeCastTarget::integer('int2'));
        self::assertSame('BIGINT', \ZtdQuery\Platform\Postgres\Sql\Value\NativeCastTarget::integer('BIGSERIAL'));
        self::assertSame('INTEGER', \ZtdQuery\Platform\Postgres\Sql\Value\NativeCastTarget::integer('INTEGER'));
    }

    public function testDecimalPreservesPrecision(): void
    {
        self::assertSame('NUMERIC(10,2)', \ZtdQuery\Platform\Postgres\Sql\Value\NativeCastTarget::decimal('decimal(10,2)'));
        self::assertSame('NUMERIC(8,0)', \ZtdQuery\Platform\Postgres\Sql\Value\NativeCastTarget::decimal('NUMERIC(8)'));
        self::assertSame('NUMERIC', \ZtdQuery\Platform\Postgres\Sql\Value\NativeCastTarget::decimal('DECIMAL'));
    }

    public function testStringPreservesBounds(): void
    {
        self::assertSame('VARCHAR(20)', \ZtdQuery\Platform\Postgres\Sql\Value\NativeCastTarget::string('varchar(20)'));
        self::assertSame('VARCHAR', \ZtdQuery\Platform\Postgres\Sql\Value\NativeCastTarget::string('CHARACTER VARYING'));
        self::assertSame('TEXT', \ZtdQuery\Platform\Postgres\Sql\Value\NativeCastTarget::string('CHAR(5)'));
    }
}
