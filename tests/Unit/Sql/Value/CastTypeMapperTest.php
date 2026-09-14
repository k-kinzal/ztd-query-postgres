<?php

declare(strict_types=1);

namespace Tests\Unit\Sql\Value;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Sql\Value\CastTypeMapper::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Value\NativeCastTarget::class)]
final class CastTypeMapperTest extends TestCase
{
    public function testMapPreservesDomainsArraysAndScalarFamilies(): void
    {
        self::assertSame('"app"."positive_int"', (new \ZtdQuery\Platform\Postgres\Sql\Value\CastTypeMapper())->map(new \ZtdQuery\Schema\ColumnDeclaration(\ZtdQuery\Schema\ColumnTypeFamily::UNKNOWN, '"app"."positive_int"')));
        self::assertSame('TEXT', (new \ZtdQuery\Platform\Postgres\Sql\Value\CastTypeMapper())->map(new \ZtdQuery\Schema\ColumnDeclaration(\ZtdQuery\Schema\ColumnTypeFamily::UNKNOWN, '')));
        self::assertSame('INTEGER[]', (new \ZtdQuery\Platform\Postgres\Sql\Value\CastTypeMapper())->map(new \ZtdQuery\Schema\ColumnDeclaration(\ZtdQuery\Schema\ColumnTypeFamily::INTEGER, ' INTEGER[] ')));
        self::assertSame('BOOLEAN', (new \ZtdQuery\Platform\Postgres\Sql\Value\CastTypeMapper())->map(new \ZtdQuery\Schema\ColumnDeclaration(\ZtdQuery\Schema\ColumnTypeFamily::BOOLEAN, 'bool')));
        self::assertSame('JSONB', (new \ZtdQuery\Platform\Postgres\Sql\Value\CastTypeMapper())->map(new \ZtdQuery\Schema\ColumnDeclaration(\ZtdQuery\Schema\ColumnTypeFamily::JSON, 'json')));
        self::assertSame('TIMESTAMP', (new \ZtdQuery\Platform\Postgres\Sql\Value\CastTypeMapper())->map(new \ZtdQuery\Schema\ColumnDeclaration(\ZtdQuery\Schema\ColumnTypeFamily::DATETIME, 'timestamp')));
        self::assertSame('VARCHAR(32)', (new \ZtdQuery\Platform\Postgres\Sql\Value\CastTypeMapper())->map(new \ZtdQuery\Schema\ColumnDeclaration(\ZtdQuery\Schema\ColumnTypeFamily::STRING, 'VARCHAR(32)')));
    }
}
