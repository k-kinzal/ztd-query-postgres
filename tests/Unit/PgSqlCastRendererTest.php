<?php

declare(strict_types=1);

namespace Tests\Unit;

use Tests\Contract\CastRendererContractTest;
use ZtdQuery\Platform\CastRenderer;
use ZtdQuery\Platform\Postgres\PgSqlCastRenderer;
use ZtdQuery\Schema\ColumnType;
use ZtdQuery\Schema\ColumnTypeFamily;
use PHPUnit\Framework\Attributes\CoversClass;

#[CoversClass(PgSqlCastRenderer::class)]
final class PgSqlCastRendererTest extends CastRendererContractTest
{
    protected function createRenderer(): CastRenderer
    {
        return new PgSqlCastRenderer();
    }

    #[\Override]
    protected function nativeTypeFor(ColumnTypeFamily $family): string
    {
        return match ($family) {
            ColumnTypeFamily::INTEGER => 'INTEGER',
            ColumnTypeFamily::FLOAT => 'REAL',
            ColumnTypeFamily::DOUBLE => 'DOUBLE PRECISION',
            ColumnTypeFamily::DECIMAL => 'NUMERIC(10,2)',
            ColumnTypeFamily::STRING => 'VARCHAR(255)',
            ColumnTypeFamily::TEXT => 'TEXT',
            ColumnTypeFamily::BOOLEAN => 'BOOLEAN',
            ColumnTypeFamily::DATE => 'DATE',
            ColumnTypeFamily::TIME => 'TIME',
            ColumnTypeFamily::DATETIME => 'TIMESTAMP',
            ColumnTypeFamily::TIMESTAMP => 'TIMESTAMPTZ',
            ColumnTypeFamily::BINARY => 'BYTEA',
            ColumnTypeFamily::JSON => 'JSONB',
            ColumnTypeFamily::UNKNOWN => 'UUID',
        };
    }

    public function testRenderCastInteger(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER');
        self::assertSame("CAST('42' AS INTEGER)", $renderer->renderCast("'42'", $type));
    }

    public function testRenderNullCastInteger(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER');
        self::assertSame('CAST(NULL AS INTEGER)', $renderer->renderNullCast($type));
    }

    public function testRenderNullCastString(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)');
        self::assertSame('CAST(NULL AS VARCHAR(255))', $renderer->renderNullCast($type));
    }

    public function testRenderNullCastText(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnType(ColumnTypeFamily::TEXT, 'TEXT');
        self::assertSame('CAST(NULL AS TEXT)', $renderer->renderNullCast($type));
    }

    public function testRenderNullCastBoolean(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnType(ColumnTypeFamily::BOOLEAN, 'BOOLEAN');
        self::assertSame('CAST(NULL AS BOOLEAN)', $renderer->renderNullCast($type));
    }

    public function testRenderNullCastFloat(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnType(ColumnTypeFamily::FLOAT, 'REAL');
        self::assertSame('CAST(NULL AS REAL)', $renderer->renderNullCast($type));
    }

    public function testRenderNullCastDouble(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnType(ColumnTypeFamily::DOUBLE, 'DOUBLE PRECISION');
        self::assertSame('CAST(NULL AS DOUBLE PRECISION)', $renderer->renderNullCast($type));
    }

    public function testRenderNullCastDecimal(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnType(ColumnTypeFamily::DECIMAL, 'NUMERIC(10,2)');
        self::assertSame('CAST(NULL AS NUMERIC(10,2))', $renderer->renderNullCast($type));
    }

    public function testRenderNullCastDate(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnType(ColumnTypeFamily::DATE, 'DATE');
        self::assertSame('CAST(NULL AS DATE)', $renderer->renderNullCast($type));
    }

    public function testRenderNullCastTime(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnType(ColumnTypeFamily::TIME, 'TIME');
        self::assertSame('CAST(NULL AS TIME)', $renderer->renderNullCast($type));
    }

    public function testRenderNullCastDatetime(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnType(ColumnTypeFamily::DATETIME, 'TIMESTAMP');
        self::assertSame('CAST(NULL AS TIMESTAMP)', $renderer->renderNullCast($type));
    }

    public function testRenderNullCastTimestamp(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnType(ColumnTypeFamily::TIMESTAMP, 'TIMESTAMPTZ');
        self::assertSame('CAST(NULL AS TIMESTAMP)', $renderer->renderNullCast($type));
    }

    public function testRenderNullCastBinary(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnType(ColumnTypeFamily::BINARY, 'BYTEA');
        self::assertSame('CAST(NULL AS BYTEA)', $renderer->renderNullCast($type));
    }

    public function testRenderNullCastJson(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnType(ColumnTypeFamily::JSON, 'JSONB');
        self::assertSame('CAST(NULL AS JSONB)', $renderer->renderNullCast($type));
    }

    public function testRenderNullCastUnknownUsesNativeType(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnType(ColumnTypeFamily::UNKNOWN, 'UUID');
        self::assertSame('CAST(NULL AS UUID)', $renderer->renderNullCast($type));
    }

    public function testRenderNullCastUnknownEmptyNativeType(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnType(ColumnTypeFamily::UNKNOWN, '');
        self::assertSame('CAST(NULL AS TEXT)', $renderer->renderNullCast($type));
    }

    public function testColumnTypeFamilyIntegerHandled(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnType(ColumnTypeFamily::INTEGER, 'SOME_TYPE');
        $result = $renderer->renderNullCast($type);
        self::assertNotEmpty($result);
        self::assertStringContainsString('CAST(', $result);
    }

    public function testColumnTypeFamilyFloatHandled(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnType(ColumnTypeFamily::FLOAT, 'SOME_TYPE');
        $result = $renderer->renderNullCast($type);
        self::assertNotEmpty($result);
        self::assertStringContainsString('CAST(', $result);
    }

    public function testColumnTypeFamilyDoubleHandled(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnType(ColumnTypeFamily::DOUBLE, 'SOME_TYPE');
        $result = $renderer->renderNullCast($type);
        self::assertNotEmpty($result);
        self::assertStringContainsString('CAST(', $result);
    }

    public function testColumnTypeFamilyDecimalHandled(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnType(ColumnTypeFamily::DECIMAL, 'SOME_TYPE');
        $result = $renderer->renderNullCast($type);
        self::assertNotEmpty($result);
        self::assertStringContainsString('CAST(', $result);
    }

    public function testColumnTypeFamilyStringHandled(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnType(ColumnTypeFamily::STRING, 'SOME_TYPE');
        $result = $renderer->renderNullCast($type);
        self::assertNotEmpty($result);
        self::assertStringContainsString('CAST(', $result);
    }

    public function testColumnTypeFamilyTextHandled(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnType(ColumnTypeFamily::TEXT, 'SOME_TYPE');
        $result = $renderer->renderNullCast($type);
        self::assertNotEmpty($result);
        self::assertStringContainsString('CAST(', $result);
    }

    public function testColumnTypeFamilyBooleanHandled(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnType(ColumnTypeFamily::BOOLEAN, 'SOME_TYPE');
        $result = $renderer->renderNullCast($type);
        self::assertNotEmpty($result);
        self::assertStringContainsString('CAST(', $result);
    }

    public function testColumnTypeFamilyDateHandled(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnType(ColumnTypeFamily::DATE, 'SOME_TYPE');
        $result = $renderer->renderNullCast($type);
        self::assertNotEmpty($result);
        self::assertStringContainsString('CAST(', $result);
    }

    public function testColumnTypeFamilyTimeHandled(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnType(ColumnTypeFamily::TIME, 'SOME_TYPE');
        $result = $renderer->renderNullCast($type);
        self::assertNotEmpty($result);
        self::assertStringContainsString('CAST(', $result);
    }

    public function testColumnTypeFamilyDatetimeHandled(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnType(ColumnTypeFamily::DATETIME, 'SOME_TYPE');
        $result = $renderer->renderNullCast($type);
        self::assertNotEmpty($result);
        self::assertStringContainsString('CAST(', $result);
    }

    public function testColumnTypeFamilyTimestampHandled(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnType(ColumnTypeFamily::TIMESTAMP, 'SOME_TYPE');
        $result = $renderer->renderNullCast($type);
        self::assertNotEmpty($result);
        self::assertStringContainsString('CAST(', $result);
    }

    public function testColumnTypeFamilyBinaryHandled(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnType(ColumnTypeFamily::BINARY, 'SOME_TYPE');
        $result = $renderer->renderNullCast($type);
        self::assertNotEmpty($result);
        self::assertStringContainsString('CAST(', $result);
    }

    public function testColumnTypeFamilyJsonHandled(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnType(ColumnTypeFamily::JSON, 'SOME_TYPE');
        $result = $renderer->renderNullCast($type);
        self::assertNotEmpty($result);
        self::assertStringContainsString('CAST(', $result);
    }

    public function testColumnTypeFamilyUnknownHandled(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnType(ColumnTypeFamily::UNKNOWN, 'SOME_TYPE');
        $result = $renderer->renderNullCast($type);
        self::assertNotEmpty($result);
        self::assertStringContainsString('CAST(', $result);
    }

    public function testRenderCastIsDeterministic(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER');
        self::assertSame(
            $renderer->renderNullCast($type),
            $renderer->renderNullCast($type)
        );
    }

    public function testRenderCastDecimalWithPrecisionAndScale(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnType(ColumnTypeFamily::DECIMAL, 'NUMERIC(10,2)');
        self::assertSame("CAST('42.5' AS NUMERIC(10,2))", $renderer->renderCast("'42.5'", $type));
    }

    public function testRenderCastDecimalWithPrecisionOnly(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnType(ColumnTypeFamily::DECIMAL, 'NUMERIC(8)');
        self::assertSame("CAST('100' AS NUMERIC(8,0))", $renderer->renderCast("'100'", $type));
    }

    public function testRenderCastDecimalBare(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnType(ColumnTypeFamily::DECIMAL, 'NUMERIC');
        self::assertSame("CAST('99' AS NUMERIC)", $renderer->renderCast("'99'", $type));
    }

    public function testRenderCastDecimalLowercaseNativeType(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnType(ColumnTypeFamily::DECIMAL, 'numeric(10,2)');
        self::assertSame("CAST('42.5' AS NUMERIC(10,2))", $renderer->renderCast("'42.5'", $type));
    }

    public function testRenderCastDecimalFromDecimalKeyword(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnType(ColumnTypeFamily::DECIMAL, 'DECIMAL(12,4)');
        self::assertSame("CAST('3.14' AS NUMERIC(12,4))", $renderer->renderCast("'3.14'", $type));
    }

    public function testRenderCastStringWithVarcharLength(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(100)');
        self::assertSame("CAST('hi' AS VARCHAR(100))", $renderer->renderCast("'hi'", $type));
    }

    public function testRenderCastStringLowercaseVarchar(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnType(ColumnTypeFamily::STRING, 'varchar(50)');
        self::assertSame("CAST('hi' AS VARCHAR(50))", $renderer->renderCast("'hi'", $type));
    }

    public function testRenderCastStringWithoutLength(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR');
        self::assertSame("CAST('hi' AS TEXT)", $renderer->renderCast("'hi'", $type));
    }

    public function testRenderCastDecimalLowercaseDecimalKeyword(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnType(ColumnTypeFamily::DECIMAL, 'decimal(5,2)');
        self::assertSame("CAST('1.0' AS NUMERIC(5,2))", $renderer->renderCast("'1.0'", $type));
    }

    public function testRenderCastDecimalPrecisionOnlyLowercase(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnType(ColumnTypeFamily::DECIMAL, 'decimal(6)');
        self::assertSame("CAST('1' AS NUMERIC(6,0))", $renderer->renderCast("'1'", $type));
    }
}
