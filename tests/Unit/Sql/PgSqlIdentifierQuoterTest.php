<?php

declare(strict_types=1);

namespace Tests\Unit\Sql;

use PHPUnit\Framework\Attributes\CoversClass;
use ZtdQuery\Platform\Postgres\Sql\PgSqlIdentifierQuoter;

#[CoversClass(PgSqlIdentifierQuoter::class)]
final class PgSqlIdentifierQuoterTest extends \PHPUnit\Framework\TestCase
{
    public function testQuoteReturnsNonEmptyString(): void
    {
        $quoter = new PgSqlIdentifierQuoter();
        $result = $quoter->quote('users');
        self::assertNotEmpty($result);
    }

    public function testQuoteWrapsIdentifier(): void
    {
        $quoter = new PgSqlIdentifierQuoter();
        $char = '"';
        $result = $quoter->quote('table_name');
        self::assertStringStartsWith($char, $result);
        self::assertStringEndsWith($char, $result);
    }

    public function testQuoteEscapesQuoteCharacterInIdentifier(): void
    {
        $quoter = new PgSqlIdentifierQuoter();
        $char = '"';
        $identifier = 'col' . $char . 'name';
        $result = $quoter->quote($identifier);
        self::assertNotEmpty($result);
        self::assertStringStartsWith($char, $result);
        self::assertStringEndsWith($char, $result);
        $simpleQuoted = $char . $identifier . $char;
        self::assertGreaterThanOrEqual(strlen($simpleQuoted), strlen($result), 'Escaped identifier should be at least as long as non-escaped form');
    }

    public function testQuoteProducesExactResult(): void
    {
        $quoter = new PgSqlIdentifierQuoter();
        $char = '"';
        $result = $quoter->quote('users');
        self::assertSame($char . 'users' . $char, $result);
    }

    public function testQuoteEscapesEmbeddedQuoteExactly(): void
    {
        $quoter = new PgSqlIdentifierQuoter();
        $char = '"';
        $result = $quoter->quote('col' . $char . 'name');
        $expected = $char . 'col' . $char . $char . 'name' . $char;
        self::assertSame($expected, $result);
    }

    public function testQuoteIsDeterministic(): void
    {
        $quoter = new PgSqlIdentifierQuoter();
        self::assertSame($quoter->quote('test_table'), $quoter->quote('test_table'));
    }

    public function testQuotedIdentifierContainsOriginalName(): void
    {
        $quoter = new PgSqlIdentifierQuoter();
        $identifier = 'column_name';
        $result = $quoter->quote($identifier);
        self::assertStringContainsString($identifier, $result);
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
