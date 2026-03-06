<?php

declare(strict_types=1);

namespace Tests\Unit;

use Tests\Contract\IdentifierQuoterContractTest;
use ZtdQuery\Platform\IdentifierQuoter;
use ZtdQuery\Platform\Postgres\PgSqlIdentifierQuoter;
use PHPUnit\Framework\Attributes\CoversClass;

#[CoversClass(PgSqlIdentifierQuoter::class)]
final class PgSqlIdentifierQuoterTest extends IdentifierQuoterContractTest
{
    protected function createQuoter(): IdentifierQuoter
    {
        return new PgSqlIdentifierQuoter();
    }

    protected function quoteCharacter(): string
    {
        return '"';
    }

    public function testQuoteSimpleIdentifier(): void
    {
        $quoter = new PgSqlIdentifierQuoter();
        self::assertSame('"users"', $quoter->quote('users'));
    }

    public function testQuoteIdentifierWithUnderscore(): void
    {
        $quoter = new PgSqlIdentifierQuoter();
        self::assertSame('"user_name"', $quoter->quote('user_name'));
    }

    public function testQuoteEscapesEmbeddedDoubleQuotes(): void
    {
        $quoter = new PgSqlIdentifierQuoter();
        self::assertSame('"""users"""', $quoter->quote('"users"'));
    }

    public function testQuoteReturnsNonEmpty(): void
    {
        $quoter = new PgSqlIdentifierQuoter();
        self::assertNotEmpty($quoter->quote('x'));
    }

    public function testQuoteStartsAndEndsWithDoubleQuote(): void
    {
        $quoter = new PgSqlIdentifierQuoter();
        $result = $quoter->quote('table_name');
        self::assertStringStartsWith('"', $result);
        self::assertStringEndsWith('"', $result);
    }

    public function testQuoteIsDeterministic(): void
    {
        $quoter = new PgSqlIdentifierQuoter();
        self::assertSame(
            $quoter->quote('test_table'),
            $quoter->quote('test_table')
        );
    }

    public function testQuoteReservedWord(): void
    {
        $quoter = new PgSqlIdentifierQuoter();
        self::assertSame('"select"', $quoter->quote('select'));
    }

    public function testQuoteWithSpaces(): void
    {
        $quoter = new PgSqlIdentifierQuoter();
        $result = $quoter->quote('my table');
        self::assertSame('"my table"', $result);
    }

    public function testQuoteEmptyString(): void
    {
        $quoter = new PgSqlIdentifierQuoter();
        $result = $quoter->quote('');
        self::assertSame('""', $result);
    }

    public function testQuoteWithNumbers(): void
    {
        $quoter = new PgSqlIdentifierQuoter();
        self::assertSame('"123"', $quoter->quote('123'));
    }
}
