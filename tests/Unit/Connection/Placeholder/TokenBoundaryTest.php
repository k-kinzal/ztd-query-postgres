<?php

declare(strict_types=1);

namespace Tests\Unit\Connection\Placeholder;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Connection\Placeholder\TokenBoundary::class)]
final class TokenBoundaryTest extends TestCase
{
    public function testKeywordExpectsOperandRecognizesContext(): void
    {
        self::assertTrue(\ZtdQuery\Platform\Postgres\Connection\Placeholder\TokenBoundary::keywordExpectsOperand('SELECT'));
        self::assertTrue(\ZtdQuery\Platform\Postgres\Connection\Placeholder\TokenBoundary::keywordExpectsOperand('AND'));
        self::assertFalse(\ZtdQuery\Platform\Postgres\Connection\Placeholder\TokenBoundary::keywordExpectsOperand('name'));
    }

    public function testIsIdentifierStartUsesAsciiLettersOrUnderscores(): void
    {
        self::assertTrue(\ZtdQuery\Platform\Postgres\Connection\Placeholder\TokenBoundary::isIdentifierStart('_'));
        self::assertTrue(\ZtdQuery\Platform\Postgres\Connection\Placeholder\TokenBoundary::isIdentifierStart('z'));
        self::assertFalse(\ZtdQuery\Platform\Postgres\Connection\Placeholder\TokenBoundary::isIdentifierStart('9'));
    }

    public function testIsIdentifierContinuationAlsoAllowsDigitsAndDollars(): void
    {
        self::assertTrue(\ZtdQuery\Platform\Postgres\Connection\Placeholder\TokenBoundary::isIdentifierContinuation('9'));
        self::assertTrue(\ZtdQuery\Platform\Postgres\Connection\Placeholder\TokenBoundary::isIdentifierContinuation('$'));
        self::assertFalse(\ZtdQuery\Platform\Postgres\Connection\Placeholder\TokenBoundary::isIdentifierContinuation('-'));
    }

    public function testIsEscapeStringStartRequiresATokenBoundary(): void
    {
        self::assertTrue(\ZtdQuery\Platform\Postgres\Connection\Placeholder\TokenBoundary::isEscapeStringStart("e'body'", 1));
        self::assertFalse(\ZtdQuery\Platform\Postgres\Connection\Placeholder\TokenBoundary::isEscapeStringStart("name'body'", 4));
    }

    public function testDollarQuoteDelimiterPreservesTheTag(): void
    {
        self::assertSame('$tag$', \ZtdQuery\Platform\Postgres\Connection\Placeholder\TokenBoundary::dollarQuoteDelimiter('prefix$tag$body$tag$', 6));
        self::assertSame('$$', \ZtdQuery\Platform\Postgres\Connection\Placeholder\TokenBoundary::dollarQuoteDelimiter('$$body$$', 0));
        self::assertNull(\ZtdQuery\Platform\Postgres\Connection\Placeholder\TokenBoundary::dollarQuoteDelimiter('$1$', 0));
    }
}
