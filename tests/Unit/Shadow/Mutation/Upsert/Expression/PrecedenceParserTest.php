<?php

declare(strict_types=1);

namespace Tests\Unit\Shadow\Mutation\Upsert\Expression;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\PrecedenceParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\ComparisonOperator::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\ExpressionCursor::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\ExpressionLexeme::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\PrimaryParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::class)]
final class PrecedenceParserTest extends TestCase
{
    public function testParseOrPreservesPrecedenceAndConsumesItsTokens(): void
    {
        $sql = 'TRUE OR FALSE AND FALSE';
        $tokens = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create())->significantTokens();
        $cursor = new \ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\ExpressionCursor($sql, 'users', $tokens);
        $expression = (new \ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\PrecedenceParser($cursor))->parseOr();
        self::assertSame(true, $expression->evaluate([], [], 'users'));
        self::assertSame(count($tokens), $cursor->index);
    }

    public function testParseAndPreservesPrecedenceAndConsumesItsTokens(): void
    {
        $sql = 'TRUE AND (FALSE OR TRUE)';
        $tokens = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create())->significantTokens();
        $cursor = new \ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\ExpressionCursor($sql, 'users', $tokens);
        $expression = (new \ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\PrecedenceParser($cursor))->parseAnd();
        self::assertSame(true, $expression->evaluate([], [], 'users'));
        self::assertSame(count($tokens), $cursor->index);
    }

    public function testParseComparisonPreservesPrecedenceAndConsumesItsTokens(): void
    {
        $sql = '2 + 3 * 4 >= 14';
        $tokens = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create())->significantTokens();
        $cursor = new \ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\ExpressionCursor($sql, 'users', $tokens);
        $expression = (new \ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\PrecedenceParser($cursor))->parseComparison();
        self::assertSame(true, $expression->evaluate([], [], 'users'));
        self::assertSame(count($tokens), $cursor->index);
    }

    public function testParseAdditivePreservesPrecedenceAndConsumesItsTokens(): void
    {
        $sql = '8 - 3 - 1 * 2';
        $tokens = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create())->significantTokens();
        $cursor = new \ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\ExpressionCursor($sql, 'users', $tokens);
        $expression = (new \ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\PrecedenceParser($cursor))->parseAdditive();
        self::assertSame(3, $expression->evaluate([], [], 'users'));
        self::assertSame(count($tokens), $cursor->index);
    }

    public function testParseMultiplicativePreservesPrecedenceAndConsumesItsTokens(): void
    {
        $sql = '12 / 3 % 3';
        $tokens = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create())->significantTokens();
        $cursor = new \ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\ExpressionCursor($sql, 'users', $tokens);
        $expression = (new \ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\PrecedenceParser($cursor))->parseMultiplicative();
        self::assertSame(1, $expression->evaluate([], [], 'users'));
        self::assertSame(count($tokens), $cursor->index);
    }

    public function testParseUnaryPreservesPrecedenceAndConsumesItsTokens(): void
    {
        $sql = 'NOT -1';
        $tokens = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create())->significantTokens();
        $cursor = new \ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\ExpressionCursor($sql, 'users', $tokens);
        $expression = (new \ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\PrecedenceParser($cursor))->parseUnary();
        self::assertSame(false, $expression->evaluate([], [], 'users'));
        self::assertSame(count($tokens), $cursor->index);
    }
}
