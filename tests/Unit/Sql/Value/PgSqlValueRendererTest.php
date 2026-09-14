<?php

declare(strict_types=1);

namespace Tests\Unit\Sql\Value;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Stringable;
use ZtdQuery\Platform\Postgres\Sql\Value\PgSqlValueRenderer;
use ZtdQuery\Schema\ColumnDeclaration;
use ZtdQuery\Schema\ColumnTypeFamily;

#[CoversClass(PgSqlValueRenderer::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Value\PgSqlCastRenderer::class)]
#[CoversClass(\ZtdQuery\Platform\Postgres\Sql\Value\BinaryStream::class)]
#[CoversClass(\ZtdQuery\Platform\Postgres\Sql\Value\LiteralText::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Value\CastTypeMapper::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Value\NativeCastTarget::class)]
final class PgSqlValueRendererTest extends TestCase
{
    public function testRenderValueFalseUsesBooleanLiteral(): void
    {
        $renderer = new PgSqlValueRenderer();

        self::assertSame(
            "CAST('0' AS BOOLEAN)",
            $renderer->renderValue(false, new ColumnDeclaration(ColumnTypeFamily::BOOLEAN, 'BOOLEAN')),
        );
    }

    public function testBigIntRetainsNativeWidth(): void
    {
        $renderer = new PgSqlValueRenderer();

        self::assertSame(
            "CAST('9223372036854775807' AS BIGINT)",
            $renderer->renderValue(9223372036854775807, new ColumnDeclaration(ColumnTypeFamily::INTEGER, 'BIGINT')),
        );
    }

    public function testArrayRetainsNativeArrayType(): void
    {
        $renderer = new PgSqlValueRenderer();

        self::assertSame(
            "CAST('{1,2,3}' AS INT4[])",
            $renderer->renderValue('{1,2,3}', new ColumnDeclaration(ColumnTypeFamily::INTEGER, 'INT4[]')),
        );
    }

    public function testBinaryUsesDecodeExpression(): void
    {
        $renderer = new PgSqlValueRenderer();

        self::assertSame(
            "CAST(decode('0001ff', 'hex') AS BYTEA)",
            $renderer->renderValue("\x00\x01\xFF", new ColumnDeclaration(ColumnTypeFamily::BINARY, 'BYTEA')),
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
            $renderer->renderValue(2.718281828459045, new ColumnDeclaration(ColumnTypeFamily::DOUBLE, 'DOUBLE PRECISION')),
        );
    }

    public function testInferredAndDeclaredStringableRemainDistinct(): void
    {
        $value = new class () implements Stringable {
            public function __toString(): string
            {
                return 'CURRENT_TIMESTAMP';
            }
        };
        $renderer = new PgSqlValueRenderer();

        self::assertSame('CURRENT_TIMESTAMP', $renderer->renderValue($value));
        self::assertSame(
            "CAST('CURRENT_TIMESTAMP' AS TEXT)",
            $renderer->renderValue($value, new ColumnDeclaration(ColumnTypeFamily::TEXT, 'TEXT')),
        );
    }

    public function testUntypedArrayIsRejected(): void
    {
        $this->expectException(RuntimeException::class);

        (new PgSqlValueRenderer())->renderValue(['value']);
    }

    public function testDeclaredNonScalarUsesSerializedRepresentation(): void
    {
        self::assertSame(
            "CAST('a:1:{i:0;s:5:\"value\";}' AS JSONB)",
            (new PgSqlValueRenderer())->renderValue(['value'], new ColumnDeclaration(ColumnTypeFamily::JSON, 'JSONB')),
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
            (new PgSqlValueRenderer())->renderValue($stream, new ColumnDeclaration(ColumnTypeFamily::BINARY, 'BYTEA')),
        );
        self::assertSame(1, ftell($stream));
        fclose($stream);
    }

    public function testBinaryRejectsNonStringableNonResource(): void
    {
        $this->expectException(RuntimeException::class);

        (new PgSqlValueRenderer())->renderValue(
            ['value'],
            new ColumnDeclaration(ColumnTypeFamily::BINARY, 'BYTEA'),
        );
    }
}
