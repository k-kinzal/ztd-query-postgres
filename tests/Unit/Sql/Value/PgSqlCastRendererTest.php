<?php

declare(strict_types=1);

namespace Tests\Unit\Sql\Value;

use PHPUnit\Framework\Attributes\CoversClass;
use ZtdQuery\Platform\Postgres\Sql\Value\PgSqlCastRenderer;
use ZtdQuery\Schema\ColumnDeclaration;
use ZtdQuery\Schema\ColumnTypeFamily;

#[CoversClass(PgSqlCastRenderer::class)]
#[CoversClass(\ZtdQuery\Platform\Postgres\Sql\Value\CastTypeMapper::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Value\NativeCastTarget::class)]
final class PgSqlCastRendererTest extends \PHPUnit\Framework\TestCase
{
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::INTEGER, 'INTEGER'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::FLOAT, 'REAL'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::DOUBLE, 'DOUBLE PRECISION'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::DECIMAL, 'NUMERIC(10,2)'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::STRING, 'VARCHAR(255)'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::TEXT, 'TEXT'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::BOOLEAN, 'BOOLEAN'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::DATE, 'DATE'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::TIME, 'TIME'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::DATETIME, 'TIMESTAMP'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::TIMESTAMP, 'TIMESTAMPTZ'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::BINARY, 'BYTEA'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::JSON, 'JSONB'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::UNKNOWN, 'UUID'])]
    public function testRenderCastReturnsNonEmptyString(ColumnTypeFamily $family, string $nativeType): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnDeclaration($family, $nativeType);
        $result = $renderer->renderCast("'test'", $type);
        self::assertNotEmpty($result, sprintf('renderCast returned empty string for family %s', $family->value));
    }
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::INTEGER, 'INTEGER'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::FLOAT, 'REAL'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::DOUBLE, 'DOUBLE PRECISION'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::DECIMAL, 'NUMERIC(10,2)'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::STRING, 'VARCHAR(255)'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::TEXT, 'TEXT'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::BOOLEAN, 'BOOLEAN'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::DATE, 'DATE'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::TIME, 'TIME'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::DATETIME, 'TIMESTAMP'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::TIMESTAMP, 'TIMESTAMPTZ'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::BINARY, 'BYTEA'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::JSON, 'JSONB'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::UNKNOWN, 'UUID'])]
    public function testRenderNullCastReturnsNonEmptyString(ColumnTypeFamily $family, string $nativeType): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnDeclaration($family, $nativeType);
        $result = $renderer->renderNullCast($type);
        self::assertNotEmpty($result, sprintf('renderNullCast returned empty string for family %s', $family->value));
    }
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::INTEGER, 'INTEGER'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::FLOAT, 'REAL'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::DOUBLE, 'DOUBLE PRECISION'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::DECIMAL, 'NUMERIC(10,2)'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::STRING, 'VARCHAR(255)'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::TEXT, 'TEXT'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::BOOLEAN, 'BOOLEAN'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::DATE, 'DATE'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::TIME, 'TIME'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::DATETIME, 'TIMESTAMP'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::TIMESTAMP, 'TIMESTAMPTZ'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::BINARY, 'BYTEA'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::JSON, 'JSONB'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::UNKNOWN, 'UUID'])]
    public function testRenderNullCastContainsCastKeyword(ColumnTypeFamily $family, string $nativeType): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnDeclaration($family, $nativeType);
        $result = $renderer->renderNullCast($type);
        self::assertStringContainsString('CAST(', strtoupper($result), sprintf('renderNullCast output for family %s does not contain CAST(', $family->value));
    }
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::INTEGER, 'INTEGER'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::FLOAT, 'REAL'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::DOUBLE, 'DOUBLE PRECISION'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::DECIMAL, 'NUMERIC(10,2)'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::STRING, 'VARCHAR(255)'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::TEXT, 'TEXT'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::BOOLEAN, 'BOOLEAN'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::DATE, 'DATE'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::TIME, 'TIME'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::DATETIME, 'TIMESTAMP'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::TIMESTAMP, 'TIMESTAMPTZ'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::BINARY, 'BYTEA'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::JSON, 'JSONB'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::UNKNOWN, 'UUID'])]
    public function testRenderNullCastContainsNullKeyword(ColumnTypeFamily $family, string $nativeType): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnDeclaration($family, $nativeType);
        $result = $renderer->renderNullCast($type);
        self::assertStringContainsString('NULL', strtoupper($result), sprintf('renderNullCast output for family %s does not contain NULL', $family->value));
    }
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::INTEGER, 'INTEGER'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::FLOAT, 'REAL'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::DOUBLE, 'DOUBLE PRECISION'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::DECIMAL, 'NUMERIC(10,2)'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::STRING, 'VARCHAR(255)'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::TEXT, 'TEXT'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::BOOLEAN, 'BOOLEAN'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::DATE, 'DATE'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::TIME, 'TIME'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::DATETIME, 'TIMESTAMP'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::TIMESTAMP, 'TIMESTAMPTZ'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::BINARY, 'BYTEA'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::JSON, 'JSONB'])]
    #[\PHPUnit\Framework\Attributes\TestWith([ColumnTypeFamily::UNKNOWN, 'UUID'])]
    public function testAllColumnTypeFamiliesHandled(ColumnTypeFamily $family, string $nativeType): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnDeclaration($family, $nativeType);
        $castResult = $renderer->renderCast("'value'", $type);
        $nullResult = $renderer->renderNullCast($type);
        self::assertNotEmpty($castResult, sprintf('renderCast returned empty for family %s', $family->value));
        self::assertNotEmpty($nullResult, sprintf('renderNullCast returned empty for family %s', $family->value));
    }
    public function testRenderCastForIntegerProducesExactForm(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnDeclaration(ColumnTypeFamily::INTEGER, 'INTEGER');
        $result = $renderer->renderCast("'42'", $type);
        self::assertMatchesRegularExpression('/^CAST\(\'42\'\s+AS\s+\S+\)$/i', $result, 'renderCast for INTEGER must produce CAST(expression AS type) format');
    }
    public function testRenderNullCastProducesExactForm(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnDeclaration(ColumnTypeFamily::INTEGER, 'INTEGER');
        $result = $renderer->renderNullCast($type);
        self::assertMatchesRegularExpression('/^CAST\(NULL\s+AS\s+\S+\)$/i', $result, 'renderNullCast must produce CAST(NULL AS type) format');
    }
    public function testRenderCastForStringIncludesStringType(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnDeclaration(ColumnTypeFamily::STRING, 'VARCHAR(255)');
        $result = $renderer->renderCast("'hello'", $type);
        $upper = strtoupper($result);
        self::assertTrue(str_contains($upper, 'VARCHAR') || str_contains($upper, 'TEXT') || str_contains($upper, 'CHAR'), sprintf('renderCast for STRING must include VARCHAR, TEXT, or CHAR, got: %s', $result));
    }
    public function testRenderCastIsDeterministic(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnDeclaration(ColumnTypeFamily::INTEGER, 'INTEGER');
        self::assertSame($renderer->renderNullCast($type), $renderer->renderNullCast($type));
    }
    public function testRenderCastInteger(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnDeclaration(ColumnTypeFamily::INTEGER, 'INTEGER');
        self::assertSame("CAST('42' AS INTEGER)", $renderer->renderCast("'42'", $type));
    }
    public function testRenderCastBigIntPreservesWidth(): void
    {
        $renderer = new PgSqlCastRenderer();
        self::assertSame('CAST(9223372036854775807 AS BIGINT)', $renderer->renderCast('9223372036854775807', new ColumnDeclaration(ColumnTypeFamily::INTEGER, 'BIGINT')));
    }
    public function testRenderCastPreservesArrayType(): void
    {
        $renderer = new PgSqlCastRenderer();
        self::assertSame("CAST('{1,2}' AS INT4[])", $renderer->renderCast("'{1,2}'", new ColumnDeclaration(ColumnTypeFamily::INTEGER, 'INT4[]')));
    }
    public function testRenderCastTrimsArrayType(): void
    {
        $renderer = new PgSqlCastRenderer();
        self::assertSame("CAST('{1,2}' AS INT4[])", $renderer->renderCast("'{1,2}'", new ColumnDeclaration(ColumnTypeFamily::INTEGER, ' INT4[] ')));
    }
    public function testRenderCastPreservesInt2Alias(): void
    {
        $renderer = new PgSqlCastRenderer();
        self::assertSame('CAST(1 AS SMALLINT)', $renderer->renderCast('1', new ColumnDeclaration(ColumnTypeFamily::INTEGER, 'int2')));
    }
    public function testRenderCastPreservesSmallIntAlias(): void
    {
        $renderer = new PgSqlCastRenderer();
        self::assertSame('CAST(1 AS SMALLINT)', $renderer->renderCast('1', new ColumnDeclaration(ColumnTypeFamily::INTEGER, 'smallint')));
    }
    public function testRenderCastPreservesSmallSerialAlias(): void
    {
        $renderer = new PgSqlCastRenderer();
        self::assertSame('CAST(1 AS SMALLINT)', $renderer->renderCast('1', new ColumnDeclaration(ColumnTypeFamily::INTEGER, 'smallserial')));
    }
    public function testRenderCastPreservesInt8Alias(): void
    {
        $renderer = new PgSqlCastRenderer();
        self::assertSame('CAST(1 AS BIGINT)', $renderer->renderCast('1', new ColumnDeclaration(ColumnTypeFamily::INTEGER, 'int8')));
    }
    public function testRenderCastPreservesBigIntAlias(): void
    {
        $renderer = new PgSqlCastRenderer();
        self::assertSame('CAST(1 AS BIGINT)', $renderer->renderCast('1', new ColumnDeclaration(ColumnTypeFamily::INTEGER, 'bigint')));
    }
    public function testRenderCastPreservesBigSerialAlias(): void
    {
        $renderer = new PgSqlCastRenderer();
        self::assertSame('CAST(1 AS BIGINT)', $renderer->renderCast('1', new ColumnDeclaration(ColumnTypeFamily::INTEGER, 'bigserial')));
    }
    public function testRenderNullCastInteger(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnDeclaration(ColumnTypeFamily::INTEGER, 'INTEGER');
        self::assertSame('CAST(NULL AS INTEGER)', $renderer->renderNullCast($type));
    }
    public function testRenderNullCastString(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnDeclaration(ColumnTypeFamily::STRING, 'VARCHAR(255)');
        self::assertSame('CAST(NULL AS VARCHAR(255))', $renderer->renderNullCast($type));
    }
    public function testRenderNullCastText(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnDeclaration(ColumnTypeFamily::TEXT, 'TEXT');
        self::assertSame('CAST(NULL AS TEXT)', $renderer->renderNullCast($type));
    }
    public function testRenderNullCastBoolean(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnDeclaration(ColumnTypeFamily::BOOLEAN, 'BOOLEAN');
        self::assertSame('CAST(NULL AS BOOLEAN)', $renderer->renderNullCast($type));
    }
    public function testRenderNullCastFloat(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnDeclaration(ColumnTypeFamily::FLOAT, 'REAL');
        self::assertSame('CAST(NULL AS REAL)', $renderer->renderNullCast($type));
    }
    public function testRenderNullCastDouble(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnDeclaration(ColumnTypeFamily::DOUBLE, 'DOUBLE PRECISION');
        self::assertSame('CAST(NULL AS DOUBLE PRECISION)', $renderer->renderNullCast($type));
    }
    public function testRenderNullCastDecimal(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnDeclaration(ColumnTypeFamily::DECIMAL, 'NUMERIC(10,2)');
        self::assertSame('CAST(NULL AS NUMERIC(10,2))', $renderer->renderNullCast($type));
    }
    public function testRenderNullCastDate(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnDeclaration(ColumnTypeFamily::DATE, 'DATE');
        self::assertSame('CAST(NULL AS DATE)', $renderer->renderNullCast($type));
    }
    public function testRenderNullCastTime(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnDeclaration(ColumnTypeFamily::TIME, 'TIME');
        self::assertSame('CAST(NULL AS TIME)', $renderer->renderNullCast($type));
    }
    public function testRenderNullCastDatetime(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnDeclaration(ColumnTypeFamily::DATETIME, 'TIMESTAMP');
        self::assertSame('CAST(NULL AS TIMESTAMP)', $renderer->renderNullCast($type));
    }
    public function testRenderNullCastTimestamp(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnDeclaration(ColumnTypeFamily::TIMESTAMP, 'TIMESTAMPTZ');
        self::assertSame('CAST(NULL AS TIMESTAMP)', $renderer->renderNullCast($type));
    }
    public function testRenderNullCastBinary(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnDeclaration(ColumnTypeFamily::BINARY, 'BYTEA');
        self::assertSame('CAST(NULL AS BYTEA)', $renderer->renderNullCast($type));
    }
    public function testRenderNullCastJson(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnDeclaration(ColumnTypeFamily::JSON, 'JSONB');
        self::assertSame('CAST(NULL AS JSONB)', $renderer->renderNullCast($type));
    }
    public function testRenderNullCastUnknownUsesNativeType(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnDeclaration(ColumnTypeFamily::UNKNOWN, 'UUID');
        self::assertSame('CAST(NULL AS UUID)', $renderer->renderNullCast($type));
    }
    public function testRenderNullCastUnknownEmptyNativeType(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnDeclaration(ColumnTypeFamily::UNKNOWN, '');
        self::assertSame('CAST(NULL AS TEXT)', $renderer->renderNullCast($type));
    }
    public function testColumnTypeFamilyIntegerHandled(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnDeclaration(ColumnTypeFamily::INTEGER, 'SOME_TYPE');
        $result = $renderer->renderNullCast($type);
        self::assertNotEmpty($result);
        self::assertStringContainsString('CAST(', $result);
    }
    public function testColumnTypeFamilyFloatHandled(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnDeclaration(ColumnTypeFamily::FLOAT, 'SOME_TYPE');
        $result = $renderer->renderNullCast($type);
        self::assertNotEmpty($result);
        self::assertStringContainsString('CAST(', $result);
    }
    public function testColumnTypeFamilyDoubleHandled(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnDeclaration(ColumnTypeFamily::DOUBLE, 'SOME_TYPE');
        $result = $renderer->renderNullCast($type);
        self::assertNotEmpty($result);
        self::assertStringContainsString('CAST(', $result);
    }
    public function testColumnTypeFamilyDecimalHandled(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnDeclaration(ColumnTypeFamily::DECIMAL, 'SOME_TYPE');
        $result = $renderer->renderNullCast($type);
        self::assertNotEmpty($result);
        self::assertStringContainsString('CAST(', $result);
    }
    public function testColumnTypeFamilyStringHandled(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnDeclaration(ColumnTypeFamily::STRING, 'SOME_TYPE');
        $result = $renderer->renderNullCast($type);
        self::assertNotEmpty($result);
        self::assertStringContainsString('CAST(', $result);
    }
    public function testColumnTypeFamilyTextHandled(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnDeclaration(ColumnTypeFamily::TEXT, 'SOME_TYPE');
        $result = $renderer->renderNullCast($type);
        self::assertNotEmpty($result);
        self::assertStringContainsString('CAST(', $result);
    }
    public function testColumnTypeFamilyBooleanHandled(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnDeclaration(ColumnTypeFamily::BOOLEAN, 'SOME_TYPE');
        $result = $renderer->renderNullCast($type);
        self::assertNotEmpty($result);
        self::assertStringContainsString('CAST(', $result);
    }
    public function testColumnTypeFamilyDateHandled(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnDeclaration(ColumnTypeFamily::DATE, 'SOME_TYPE');
        $result = $renderer->renderNullCast($type);
        self::assertNotEmpty($result);
        self::assertStringContainsString('CAST(', $result);
    }
    public function testColumnTypeFamilyTimeHandled(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnDeclaration(ColumnTypeFamily::TIME, 'SOME_TYPE');
        $result = $renderer->renderNullCast($type);
        self::assertNotEmpty($result);
        self::assertStringContainsString('CAST(', $result);
    }
    public function testColumnTypeFamilyDatetimeHandled(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnDeclaration(ColumnTypeFamily::DATETIME, 'SOME_TYPE');
        $result = $renderer->renderNullCast($type);
        self::assertNotEmpty($result);
        self::assertStringContainsString('CAST(', $result);
    }
    public function testColumnTypeFamilyTimestampHandled(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnDeclaration(ColumnTypeFamily::TIMESTAMP, 'SOME_TYPE');
        $result = $renderer->renderNullCast($type);
        self::assertNotEmpty($result);
        self::assertStringContainsString('CAST(', $result);
    }
    public function testColumnTypeFamilyBinaryHandled(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnDeclaration(ColumnTypeFamily::BINARY, 'SOME_TYPE');
        $result = $renderer->renderNullCast($type);
        self::assertNotEmpty($result);
        self::assertStringContainsString('CAST(', $result);
    }
    public function testColumnTypeFamilyJsonHandled(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnDeclaration(ColumnTypeFamily::JSON, 'SOME_TYPE');
        $result = $renderer->renderNullCast($type);
        self::assertNotEmpty($result);
        self::assertStringContainsString('CAST(', $result);
    }
    public function testColumnTypeFamilyUnknownHandled(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnDeclaration(ColumnTypeFamily::UNKNOWN, 'SOME_TYPE');
        $result = $renderer->renderNullCast($type);
        self::assertNotEmpty($result);
        self::assertStringContainsString('CAST(', $result);
    }
    public function testRenderCastDecimalWithPrecisionAndScale(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnDeclaration(ColumnTypeFamily::DECIMAL, 'NUMERIC(10,2)');
        self::assertSame("CAST('42.5' AS NUMERIC(10,2))", $renderer->renderCast("'42.5'", $type));
    }
    public function testRenderCastDecimalWithPrecisionOnly(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnDeclaration(ColumnTypeFamily::DECIMAL, 'NUMERIC(8)');
        self::assertSame("CAST('100' AS NUMERIC(8,0))", $renderer->renderCast("'100'", $type));
    }
    public function testRenderCastDecimalBare(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnDeclaration(ColumnTypeFamily::DECIMAL, 'NUMERIC');
        self::assertSame("CAST('99' AS NUMERIC)", $renderer->renderCast("'99'", $type));
    }
    public function testRenderCastDecimalLowercaseNativeType(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnDeclaration(ColumnTypeFamily::DECIMAL, 'numeric(10,2)');
        self::assertSame("CAST('42.5' AS NUMERIC(10,2))", $renderer->renderCast("'42.5'", $type));
    }
    public function testRenderCastDecimalFromDecimalKeyword(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnDeclaration(ColumnTypeFamily::DECIMAL, 'DECIMAL(12,4)');
        self::assertSame("CAST('3.14' AS NUMERIC(12,4))", $renderer->renderCast("'3.14'", $type));
    }
    public function testRenderCastStringWithVarcharLength(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnDeclaration(ColumnTypeFamily::STRING, 'VARCHAR(100)');
        self::assertSame("CAST('hi' AS VARCHAR(100))", $renderer->renderCast("'hi'", $type));
    }
    public function testRenderCastStringLowercaseVarchar(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnDeclaration(ColumnTypeFamily::STRING, 'varchar(50)');
        self::assertSame("CAST('hi' AS VARCHAR(50))", $renderer->renderCast("'hi'", $type));
    }
    public function testRenderCastStringWithoutLength(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnDeclaration(ColumnTypeFamily::STRING, 'VARCHAR');
        self::assertSame("CAST('hi' AS VARCHAR)", $renderer->renderCast("'hi'", $type));
    }
    public function testRenderCastCharacterVaryingWithoutLength(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnDeclaration(ColumnTypeFamily::STRING, 'CHARACTER VARYING');
        self::assertSame("CAST('hi' AS VARCHAR)", $renderer->renderCast("'hi'", $type));
    }
    public function testRenderCastUnknownStringNativeTypeFallsBackToText(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnDeclaration(ColumnTypeFamily::STRING, 'CUSTOM_STRING');
        self::assertSame("CAST('hi' AS TEXT)", $renderer->renderCast("'hi'", $type));
    }
    public function testRenderCastDecimalLowercaseDecimalKeyword(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnDeclaration(ColumnTypeFamily::DECIMAL, 'decimal(5,2)');
        self::assertSame("CAST('1.0' AS NUMERIC(5,2))", $renderer->renderCast("'1.0'", $type));
    }
    public function testRenderCastDecimalPrecisionOnlyLowercase(): void
    {
        $renderer = new PgSqlCastRenderer();
        $type = new ColumnDeclaration(ColumnTypeFamily::DECIMAL, 'decimal(6)');
        self::assertSame("CAST('1' AS NUMERIC(6,0))", $renderer->renderCast("'1'", $type));
    }
}
