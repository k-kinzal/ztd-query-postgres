<?php

declare(strict_types=1);

namespace Tests\Unit\Sql\Value;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Sql\Value\BinaryStream::class)]
final class BinaryStreamTest extends TestCase
{
    public function testReadRestoresTheStreamPosition(): void
    {
        $stream = fopen('php://memory', 'r+');
        self::assertNotFalse($stream);
        fwrite($stream, "a\0b");
        fseek($stream, 2);
        self::assertSame("a\0b", (new \ZtdQuery\Platform\Postgres\Sql\Value\BinaryStream())->read($stream));
        self::assertSame(2, ftell($stream));
        fclose($stream);
    }
}
