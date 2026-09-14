<?php

declare(strict_types=1);

namespace Tests\Unit\Schema\Reflection\Column;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Schema\Reflection\Column\NativeTypeSql::class)]
final class NativeTypeSqlTest extends TestCase
{
    public function testBuildTypeSql(): void
    {
        self::assertSame('INTEGER', (new \ZtdQuery\Platform\Postgres\Schema\Reflection\Column\NativeTypeSql())->buildTypeSql('INTEGER', 'INT4', []));

        self::assertSame('MOOD', (new \ZtdQuery\Platform\Postgres\Schema\Reflection\Column\NativeTypeSql())->buildTypeSql('USER-DEFINED', 'MOOD', []));

        self::assertSame('INT4[]', (new \ZtdQuery\Platform\Postgres\Schema\Reflection\Column\NativeTypeSql())->buildTypeSql('ARRAY', '_INT4', []));

        self::assertSame('VARCHAR(64)', (new \ZtdQuery\Platform\Postgres\Schema\Reflection\Column\NativeTypeSql())->buildTypeSql('CHARACTER VARYING', 'VARCHAR', ['character_maximum_length' => 64]));
    }

    public function testBuildVarcharType(): void
    {
        self::assertSame('VARCHAR(24)', (new \ZtdQuery\Platform\Postgres\Schema\Reflection\Column\NativeTypeSql())->buildVarcharType(['character_maximum_length' => 24]));

        self::assertSame('VARCHAR', (new \ZtdQuery\Platform\Postgres\Schema\Reflection\Column\NativeTypeSql())->buildVarcharType([]));
    }

    public function testBuildCharType(): void
    {
        self::assertSame('CHAR(24)', (new \ZtdQuery\Platform\Postgres\Schema\Reflection\Column\NativeTypeSql())->buildCharType(['character_maximum_length' => 24]));

        self::assertSame('CHAR(1)', (new \ZtdQuery\Platform\Postgres\Schema\Reflection\Column\NativeTypeSql())->buildCharType([]));
    }

    public function testBuildBitType(): void
    {
        self::assertSame('BIT(8)', (new \ZtdQuery\Platform\Postgres\Schema\Reflection\Column\NativeTypeSql())->buildBitType('BIT', ['character_maximum_length' => 8]));

        self::assertSame('BIT VARYING', (new \ZtdQuery\Platform\Postgres\Schema\Reflection\Column\NativeTypeSql())->buildBitType('BIT VARYING', []));
    }

    public function testBuildNumericType(): void
    {
        self::assertSame('NUMERIC(10,2)', (new \ZtdQuery\Platform\Postgres\Schema\Reflection\Column\NativeTypeSql())->buildNumericType(['numeric_precision' => 10, 'numeric_scale' => 2]));

        self::assertSame('NUMERIC', (new \ZtdQuery\Platform\Postgres\Schema\Reflection\Column\NativeTypeSql())->buildNumericType([]));
    }

    public function testResolveUserDefinedType(): void
    {
        self::assertSame('CITEXT', (new \ZtdQuery\Platform\Postgres\Schema\Reflection\Column\NativeTypeSql())->resolveUserDefinedType('CITEXT'));

        self::assertSame('MOOD', (new \ZtdQuery\Platform\Postgres\Schema\Reflection\Column\NativeTypeSql())->resolveUserDefinedType('MOOD'));
    }

    public function testResolveArrayType(): void
    {
        self::assertSame('INT4[]', (new \ZtdQuery\Platform\Postgres\Schema\Reflection\Column\NativeTypeSql())->resolveArrayType('_INT4'));

        self::assertSame('TEXT[]', (new \ZtdQuery\Platform\Postgres\Schema\Reflection\Column\NativeTypeSql())->resolveArrayType('_TEXT'));

        self::assertSame('MOOD[]', (new \ZtdQuery\Platform\Postgres\Schema\Reflection\Column\NativeTypeSql())->resolveArrayType('MOOD'));
    }
}
