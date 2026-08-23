<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Tests\Unit;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Exception\UnsupportedSqlException;
use ZtdQuery\Platform\Postgres\PgSqlUpsertExpressionParser;
use ZtdQuery\Sql\SqlToken;
use ZtdQuery\Sql\SqlTokenKind;

#[CoversClass(PgSqlUpsertExpressionParser::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlLexerProfile::class)]
final class PgSqlUpsertExpressionParserTest extends TestCase
{
    #[DataProvider('providerPostgresExpressionCases')]
    public function testParsesPostgresExpressionCases(string $sql, mixed $expected): void
    {
        self::assertSame(
            $expected,
            (new PgSqlUpsertExpressionParser())->parse($sql, 'items')->evaluate([], [], 'items'),
        );
    }

    /** @return iterable<string, array{string, mixed}> */
    public static function providerPostgresExpressionCases(): iterable
    {
        yield 'chained or' => ['1 OR 0 OR 0', true];
        yield 'chained and' => ['1 AND 1 AND 0', false];
        yield 'equal' => ['1 = 2', false];
        yield 'bang not equal' => ['1 != 2', true];
        yield 'angle not equal' => ['1 <> 2', true];
        yield 'less' => ['1 < 2', true];
        yield 'less or equal' => ['1 <= 2', true];
        yield 'greater' => ['2 > 1', true];
        yield 'greater or equal' => ['2 >= 1', true];
        yield 'chained additive' => ['10 - 3 + 2', 9];
        yield 'chained multiplicative' => ['20 / 5 % 3 * 2', 2];
        yield 'nested unary minus' => ['- -2', 2];
        yield 'nested unary plus' => ['+ +2', 2];
        yield 'unary minus' => ['-2', -2];
        yield 'unary plus' => ['+2', 2];
        yield 'nested not' => ['NOT NOT TRUE', true];
        yield 'parenthesized precedence' => ['(1 + 2) * 3', 9];
        yield 'null' => ['NULL', null];
        yield 'false' => ['FALSE', false];
        yield 'underscored integer' => ['1_000', 1000];
        yield 'exponent' => ['1.5e1', 15.0];
        yield 'escaped string' => ["'it''s'", "it's"];
    }

    public function testParsesExcludedAndQuotedExistingReferences(): void
    {
        $expression = (new PgSqlUpsertExpressionParser())->parse(
            '"items"."Quantity" + EXCLUDED."Quantity" * 2',
            'items',
        );

        self::assertSame(11, $expression->evaluate(['Quantity' => 5], ['Quantity' => 3], 'items'));
    }

    public function testUnescapesDoubledQuotesInIdentifiers(): void
    {
        $expression = (new PgSqlUpsertExpressionParser())->parse(
            '"it""ems"."quan""tity" + EXCLUDED."quan""tity"',
            'it"ems',
        );

        self::assertSame(8, $expression->evaluate(['quan"tity' => 5], ['quan"tity' => 3], 'it"ems'));
    }

    public function testParsesBooleanPredicateAndSqlString(): void
    {
        $expression = (new PgSqlUpsertExpressionParser())->parse(
            "score >= 80 AND EXCLUDED.name <> 'it''s blocked'",
            'items',
        );

        self::assertTrue($expression->matches(['score' => 80], ['name' => 'ready'], 'items'));
    }

    public function testReturnsNullForUnsupportedFunction(): void
    {
        self::assertNull((new PgSqlUpsertExpressionParser())->parseIfSupported('COALESCE(score, 0)', 'items'));
    }

    public function testRejectsMySqlValuesFunction(): void
    {
        $this->expectException(UnsupportedSqlException::class);

        (new PgSqlUpsertExpressionParser())->parse('VALUES(quantity)', 'items');
    }

    public function testRejectsMySqlQuotedIdentifier(): void
    {
        $this->expectException(UnsupportedSqlException::class);

        (new PgSqlUpsertExpressionParser())->parse('EXCLUDED.`quantity`', 'items');
    }

    public function testIdentifierRejectsNonPostgresQuotedToken(): void
    {
        $method = new \ReflectionMethod(PgSqlUpsertExpressionParser::class, 'isIdentifier');
        $token = new SqlToken(SqlTokenKind::QuotedIdentifier, '`quantity`', 0, 0, 0);

        self::assertFalse($method->invoke(new PgSqlUpsertExpressionParser(), $token));
    }

    #[DataProvider('providerInvalidPostgresExpression')]
    public function testRejectsInvalidPostgresExpression(string $sql): void
    {
        self::assertNull((new PgSqlUpsertExpressionParser())->parseIfSupported($sql, 'items'));
    }

    /** @return iterable<string, array{string}> */
    public static function providerInvalidPostgresExpression(): iterable
    {
        yield 'empty' => [''];
        yield 'missing close parenthesis' => ['(1 + 2'];
        yield 'wrong close parenthesis' => ['(1 + 2 value'];
        yield 'extra close parenthesis' => ['1 + 2)'];
        yield 'missing additive operand' => ['1 +'];
        yield 'missing comparison operand' => ['1 ='];
        yield 'missing qualified column' => ['items.'];
        yield 'invalid qualified column' => ['items.+'];
        yield 'invalid comparison pair' => ['1 ! 2'];
        yield 'double equals' => ['1 == 1'];
        yield 'unknown qualifier' => ['other.value'];
        yield 'symbol primary' => ['*'];
    }
}
