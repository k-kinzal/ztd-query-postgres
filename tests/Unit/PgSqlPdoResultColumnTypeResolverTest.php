<?php

declare(strict_types=1);

namespace Tests\Unit;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Platform\Postgres\PgSqlPdoResultColumnTypeResolver;
use ZtdQuery\Platform\Postgres\PgSqlColumnTypeMapper;
use ZtdQuery\Schema\ColumnTypeFamily;

#[CoversClass(PgSqlPdoResultColumnTypeResolver::class)]
#[UsesClass(PgSqlColumnTypeMapper::class)]
final class PgSqlPdoResultColumnTypeResolverTest extends TestCase
{
    public function testRestoresPostgreSqlCharacterTypeLength(): void
    {
        $resolver = new PgSqlPdoResultColumnTypeResolver();

        $varchar = $resolver->resolve(['native_type' => 'varchar', 'len' => -1, 'precision' => 34]);
        $char = $resolver->resolve(['native_type' => 'bpchar', 'len' => -1, 'precision' => 8]);

        self::assertSame(ColumnTypeFamily::STRING, $varchar->family);
        self::assertSame('VARCHAR(30)', $varchar->nativeType);
        self::assertSame(ColumnTypeFamily::STRING, $char->family);
        self::assertSame('CHAR(4)', $char->nativeType);
        self::assertSame('CHAR(1)', $resolver->resolve(['native_type' => 'bpchar', 'precision' => 5])->nativeType);
    }

    public function testPreservesTypesWithoutApplicableLength(): void
    {
        $resolver = new PgSqlPdoResultColumnTypeResolver();

        self::assertSame('int4', $resolver->resolve(['native_type' => 'int4', 'precision' => 8])->nativeType);
        self::assertSame('VARCHAR(8)', $resolver->resolve(['native_type' => 'VARCHAR(8)', 'precision' => 24])->nativeType);
        self::assertSame('varchar', $resolver->resolve(['native_type' => 'varchar', 'precision' => -1])->nativeType);
        self::assertSame('varchar', $resolver->resolve(['native_type' => 'varchar', 'precision' => 0])->nativeType);
        self::assertSame('varchar', $resolver->resolve(['native_type' => 'varchar', 'precision' => 4])->nativeType);
        self::assertSame('varchar', $resolver->resolve(['native_type' => 'varchar', 'precision' => '8'])->nativeType);
        self::assertSame('varchar', $resolver->resolve(['native_type' => 'varchar'])->nativeType);
        self::assertSame('', $resolver->resolve(['native_type' => null])->nativeType);
    }

    public function testNormalizesPostgreSqlDriverArrayTypeNames(): void
    {
        $resolver = new PgSqlPdoResultColumnTypeResolver();

        $integers = $resolver->resolve(['native_type' => '_int4', 'precision' => -1]);
        $strings = $resolver->resolve(['native_type' => '_varchar', 'precision' => -1]);

        self::assertSame(ColumnTypeFamily::INTEGER, $integers->family);
        self::assertSame('int4[]', $integers->nativeType);
        self::assertSame(ColumnTypeFamily::STRING, $strings->family);
        self::assertSame('varchar[]', $strings->nativeType);
    }
}
