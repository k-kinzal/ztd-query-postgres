<?php

declare(strict_types=1);

namespace Tests\Unit\Shadow\Mutation\Upsert\Expression;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\ExpressionLexeme::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::class)]
final class ExpressionLexemeTest extends TestCase
{
    public function testIsSymbolRequiresTheSymbolTokenKind(): void
    {
        $sql = '+ word';
        $tokens = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create())->significantTokens();
        self::assertTrue((new \ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\ExpressionLexeme())->isSymbol($tokens[0], ['+']));
        self::assertFalse((new \ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\ExpressionLexeme())->isSymbol($tokens[1], ['word']));
    }

    public function testIsIdentifierRecognizesBareAndQuotedNames(): void
    {
        $sql = 'name "Quoted" 1';
        $tokens = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create())->significantTokens();
        self::assertTrue((new \ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\ExpressionLexeme())->isIdentifier($tokens[0]));
        self::assertTrue((new \ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\ExpressionLexeme())->isIdentifier($tokens[1]));
        self::assertFalse((new \ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\ExpressionLexeme())->isIdentifier($tokens[2]));
    }

    public function testIdentifierUnescapesDoubledQuotes(): void
    {
        $sql = '"odd""name"';
        $tokens = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create())->significantTokens();
        self::assertSame('odd"name', (new \ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\ExpressionLexeme())->identifier($tokens[0]));
    }

    public function testNumberDistinguishesIntegersAndExponentLiterals(): void
    {
        self::assertSame(1000, (new \ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\ExpressionLexeme())->number('1_000'));
        self::assertSame(150.0, (new \ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\ExpressionLexeme())->number('1.5e2'));
    }

    public function testStringUnescapesDoubledApostrophes(): void
    {
        self::assertSame("it's", (new \ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\ExpressionLexeme())->string("'it''s'"));
    }
}
