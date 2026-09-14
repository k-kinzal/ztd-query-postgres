<?php

declare(strict_types=1);

namespace Tests\Unit\Sql\Lexing;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Sql\Lexing\CommentSpan::class)]
final class CommentSpanTest extends TestCase
{
    public function testEndHandlesNestedAndUnterminatedComments(): void
    {
        self::assertSame(17, \ZtdQuery\Platform\Postgres\Sql\Lexing\CommentSpan::end('/* a /* b */ c */tail', 0));
        self::assertSame(7, \ZtdQuery\Platform\Postgres\Sql\Lexing\CommentSpan::end('/* open', 0));
    }
}
