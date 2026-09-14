<?php

declare(strict_types=1);

namespace Tests\Unit\Sql\Lexing;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Sql\Lexing\QuotedSpan::class)]
final class QuotedSpanTest extends TestCase
{
    public function testQuotedLengthPreservesEscapedAndDoubledQuotes(): void
    {
        self::assertSame(6, \ZtdQuery\Platform\Postgres\Sql\Lexing\QuotedSpan::quotedLength('\'a\'\'b\' tail', '\'', false));
        self::assertSame(6, \ZtdQuery\Platform\Postgres\Sql\Lexing\QuotedSpan::quotedLength('\'a\\\'b\' tail', '\'', true));
        self::assertSame(6, \ZtdQuery\Platform\Postgres\Sql\Lexing\QuotedSpan::quotedLength('"name" rest', '"', false));
        self::assertSame(5, \ZtdQuery\Platform\Postgres\Sql\Lexing\QuotedSpan::quotedLength('\'open', '\'', false));
    }

    public function testDollarQuotedLengthIncludesBothDelimiters(): void
    {
        self::assertSame(7, \ZtdQuery\Platform\Postgres\Sql\Lexing\QuotedSpan::dollarQuotedLength('$x$a$x$'));
        self::assertNull(\ZtdQuery\Platform\Postgres\Sql\Lexing\QuotedSpan::dollarQuotedLength('$1$'));
        self::assertSame(7, \ZtdQuery\Platform\Postgres\Sql\Lexing\QuotedSpan::dollarQuotedLength('$x$open'));
    }

    public function testDollarQuoteDelimiterValidatesTag(): void
    {
        self::assertSame('$$', \ZtdQuery\Platform\Postgres\Sql\Lexing\QuotedSpan::dollarQuoteDelimiter('$$body$$'));
        self::assertSame('$tag_1$', \ZtdQuery\Platform\Postgres\Sql\Lexing\QuotedSpan::dollarQuoteDelimiter('$tag_1$body'));
        self::assertNull(\ZtdQuery\Platform\Postgres\Sql\Lexing\QuotedSpan::dollarQuoteDelimiter('$1bad$'));
    }

    public function testIsEscapeStringStartRequiresASeparatePrefix(): void
    {
        self::assertTrue(\ZtdQuery\Platform\Postgres\Sql\Lexing\QuotedSpan::isEscapeStringStart("E'abc'", 1));
        self::assertFalse(\ZtdQuery\Platform\Postgres\Sql\Lexing\QuotedSpan::isEscapeStringStart("nameE'abc'", 5));
    }
}
