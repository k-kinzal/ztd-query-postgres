<?php

declare(strict_types=1);

namespace Tests\Unit;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Platform\Postgres\PgSqlValueRenderer;
use ZtdQuery\Schema\ColumnType;
use ZtdQuery\Schema\ColumnTypeFamily;

#[CoversClass(PgSqlValueRenderer::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlCastRenderer::class)]
final class PgSqlValueRendererTest extends TestCase
{
    public function testFalseUsesBooleanLiteral(): void
    {
        $renderer = new PgSqlValueRenderer();

        self::assertSame(
            "CAST('0' AS BOOLEAN)",
            $renderer->renderValue(false, new ColumnType(ColumnTypeFamily::BOOLEAN, 'BOOLEAN')),
        );
    }

    public function testBigIntRetainsNativeWidth(): void
    {
        $renderer = new PgSqlValueRenderer();

        self::assertSame(
            "CAST('9223372036854775807' AS BIGINT)",
            $renderer->renderValue(9223372036854775807, new ColumnType(ColumnTypeFamily::INTEGER, 'BIGINT')),
        );
    }

    public function testArrayRetainsNativeArrayType(): void
    {
        $renderer = new PgSqlValueRenderer();

        self::assertSame(
            "CAST('{1,2,3}' AS INT4[])",
            $renderer->renderValue('{1,2,3}', new ColumnType(ColumnTypeFamily::INTEGER, 'INT4[]')),
        );
    }

    public function testBinaryUsesDecodeExpression(): void
    {
        $renderer = new PgSqlValueRenderer();

        self::assertSame(
            "CAST(decode('0001ff', 'hex') AS BYTEA)",
            $renderer->renderValue("\x00\x01\xFF", new ColumnType(ColumnTypeFamily::BINARY, 'BYTEA')),
        );
    }

    public function testStringQuotesApostrophes(): void
    {
        $renderer = new PgSqlValueRenderer();

        self::assertSame("CAST('O''Reilly' AS TEXT)", $renderer->renderValue("O'Reilly"));
    }

    public function testInferredScalarTypesUseNativeLiterals(): void
    {
        $renderer = new PgSqlValueRenderer();

        self::assertSame('TRUE', $renderer->renderValue(true));
        self::assertSame('2.718281828459045', $renderer->renderValue(2.718281828459045));
        self::assertSame('CAST(42 AS INTEGER)', $renderer->renderValue(42));
    }

    public function testDeclaredFloatUsesQuotedRoundTripRepresentation(): void
    {
        $renderer = new PgSqlValueRenderer();

        self::assertSame(
            "CAST('2.718281828459045' AS DOUBLE PRECISION)",
            $renderer->renderValue(2.718281828459045, new ColumnType(ColumnTypeFamily::DOUBLE, 'DOUBLE PRECISION')),
        );
    }

    public function testInferredAndDeclaredStringableRemainDistinct(): void
    {
        $value = new class () implements \Stringable {
            public function __toString(): string
            {
                return 'CURRENT_TIMESTAMP';
            }
        };
        $renderer = new PgSqlValueRenderer();

        self::assertSame('CURRENT_TIMESTAMP', $renderer->renderValue($value));
        self::assertSame(
            "CAST('CURRENT_TIMESTAMP' AS TEXT)",
            $renderer->renderValue($value, new ColumnType(ColumnTypeFamily::TEXT, 'TEXT')),
        );
    }

    public function testUntypedArrayIsRejected(): void
    {
        $this->expectException(\RuntimeException::class);

        (new PgSqlValueRenderer())->renderValue(['value']);
    }

    public function testDeclaredNonScalarUsesSerializedRepresentation(): void
    {
        self::assertSame(
            "CAST('a:1:{i:0;s:5:\"value\";}' AS JSONB)",
            (new PgSqlValueRenderer())->renderValue(['value'], new ColumnType(ColumnTypeFamily::JSON, 'JSONB')),
        );
    }

    public function testBinaryStreamIsReadWithoutChangingItsPosition(): void
    {
        $stream = fopen('php://memory', 'r+');
        self::assertIsResource($stream);
        fwrite($stream, "\x00\x01\xFF");
        fseek($stream, 1);

        self::assertSame(
            "CAST(decode('0001ff', 'hex') AS BYTEA)",
            (new PgSqlValueRenderer())->renderValue($stream, new ColumnType(ColumnTypeFamily::BINARY, 'BYTEA')),
        );
        self::assertSame(1, ftell($stream));
        fclose($stream);
    }

    public function testBinaryRejectsNonStringableNonResource(): void
    {
        $this->expectException(\RuntimeException::class);

        (new PgSqlValueRenderer())->renderValue(
            ['value'],
            new ColumnType(ColumnTypeFamily::BINARY, 'BYTEA'),
        );
    }
}
