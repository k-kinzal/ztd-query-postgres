<?php

declare(strict_types=1);

namespace Tests\Unit;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Platform\Postgres\PgSqlColumnTypeMapper;
use ZtdQuery\Schema\ColumnTypeFamily;

#[CoversClass(PgSqlColumnTypeMapper::class)]
final class PgSqlColumnTypeMapperTest extends TestCase
{
    public function testMapsSchemaAndPdoNativeTypes(): void
    {
        $mapper = new PgSqlColumnTypeMapper();

        self::assertSame(ColumnTypeFamily::INTEGER, $mapper->map('int4')->family);
        self::assertSame(ColumnTypeFamily::DOUBLE, $mapper->map('float8')->family);
        self::assertSame(ColumnTypeFamily::STRING, $mapper->map('bpchar')->family);
        self::assertSame(ColumnTypeFamily::TEXT, $mapper->map('citext')->family);
        self::assertSame(ColumnTypeFamily::BINARY, $mapper->map('bytea')->family);
        self::assertSame(ColumnTypeFamily::UNKNOWN, $mapper->map('custom_domain')->family);
    }

    public function testMapsArrayByItsElementTypeAndPreservesNativeType(): void
    {
        $type = (new PgSqlColumnTypeMapper())->map('numeric(10, 2)[][]');

        self::assertSame(ColumnTypeFamily::DECIMAL, $type->family);
        self::assertSame('numeric(10, 2)[][]', $type->nativeType);
    }

    public function testMapsEverySupportedNativeTypeAlias(): void
    {
        $families = array_merge(
            array_fill_keys(['INT', 'INT2', 'INT4', 'INT8', 'INTEGER', 'SMALLINT', 'BIGINT', 'SERIAL', 'SMALLSERIAL', 'BIGSERIAL'], ColumnTypeFamily::INTEGER),
            array_fill_keys(['REAL', 'FLOAT4'], ColumnTypeFamily::FLOAT),
            array_fill_keys(['DOUBLE PRECISION', 'FLOAT8'], ColumnTypeFamily::DOUBLE),
            array_fill_keys(['DECIMAL', 'NUMERIC'], ColumnTypeFamily::DECIMAL),
            array_fill_keys(['CHAR', 'CHARACTER', 'BPCHAR', 'VARCHAR', 'CHARACTER VARYING', 'NAME'], ColumnTypeFamily::STRING),
            array_fill_keys(['TEXT', 'CITEXT'], ColumnTypeFamily::TEXT),
            array_fill_keys(['BOOLEAN', 'BOOL'], ColumnTypeFamily::BOOLEAN),
            array_fill_keys(['DATE'], ColumnTypeFamily::DATE),
            array_fill_keys(['TIME', 'TIMETZ', 'TIME WITH TIME ZONE', 'TIME WITHOUT TIME ZONE'], ColumnTypeFamily::TIME),
            array_fill_keys(['TIMESTAMP', 'TIMESTAMPTZ', 'TIMESTAMP WITH TIME ZONE', 'TIMESTAMP WITHOUT TIME ZONE'], ColumnTypeFamily::TIMESTAMP),
            array_fill_keys(['BYTEA'], ColumnTypeFamily::BINARY),
            array_fill_keys(['JSON', 'JSONB'], ColumnTypeFamily::JSON),
        );
        $mapper = new PgSqlColumnTypeMapper();

        self::assertSame(
            array_values($families),
            array_map(static fn (string $nativeType): ColumnTypeFamily => $mapper->map($nativeType)->family, array_keys($families)),
        );
        self::assertSame(ColumnTypeFamily::INTEGER, $mapper->map(' int4[][] ')->family);
    }
}
