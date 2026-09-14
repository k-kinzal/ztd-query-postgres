<?php

declare(strict_types=1);

namespace Tests\Unit\Connection\Placeholder;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Connection\Placeholder\EscapeCursor::class)]
final class EscapeCursorTest extends TestCase
{
    public function testPreserveAdvancesByTheCopiedSpan(): void
    {
        $cursor = new \ZtdQuery\Platform\Postgres\Connection\Placeholder\EscapeCursor('SELECT ?');
        $cursor->preserve(7);
        self::assertSame('SELECT ', $cursor->result);
        self::assertSame(7, $cursor->position);
        self::assertTrue($cursor->expectsOperand);
    }
}
