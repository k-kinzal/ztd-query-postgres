<?php

declare(strict_types=1);

namespace Tests\Unit\Sql\Value;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Sql\Value\LiteralText::class)]
final class LiteralTextTest extends TestCase
{
    public function testQuotedEscapesScalars(): void
    {
        $text = new \ZtdQuery\Platform\Postgres\Sql\Value\LiteralText();
        self::assertSame("'O''Reilly'", $text->quoted("O'Reilly"));
        self::assertSame("'0'", $text->quoted(false));
        self::assertSame("'1.5'", $text->quoted(1.5));
    }

    public function testUntypedPreservesNativeLiteralExpressions(): void
    {
        $text = new \ZtdQuery\Platform\Postgres\Sql\Value\LiteralText();
        self::assertSame('TRUE', $text->untyped(true));
        self::assertSame('FALSE', $text->untyped(false));
        self::assertSame('1.5', $text->untyped(1.5));
    }
}
