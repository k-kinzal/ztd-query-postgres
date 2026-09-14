<?php

declare(strict_types=1);

namespace Tests\Unit\Sql\Cte;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Sql\Cte\HeaderParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::class)]
final class HeaderParserTest extends TestCase
{
    #[\PHPUnit\Framework\Attributes\TestWith(['SELECT 1', 'SELECT 1'])]
    #[\PHPUnit\Framework\Attributes\TestWith(['WITH x AS (SELECT 1) SELECT * FROM x', 'SELECT * FROM x'])]
    #[\PHPUnit\Framework\Attributes\TestWith(['WITH RECURSIVE x AS (SELECT 1) SELECT * FROM x', 'SELECT * FROM x'])]
    #[\PHPUnit\Framework\Attributes\TestWith(['WITH x AS MATERIALIZED (SELECT 1) SELECT * FROM x', 'SELECT * FROM x'])]
    #[\PHPUnit\Framework\Attributes\TestWith(['WITH x AS (', 'WITH x AS ('])]
    public function testStatementSqlExtractsTheBodyAfterCompleteCteDeclarations(string $sql, string $expected): void
    {
        self::assertSame($expected, (new \ZtdQuery\Platform\Postgres\Sql\Cte\HeaderParser())->statementSql($sql));
    }

    public function testParseHeader(): void
    {
        $sql = 'WITH recent AS (SELECT id FROM users) SELECT * FROM recent';
        $stream = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create());
        $tokens = $stream->significantTokens();

        self::assertSame(['names' => ['recent'], 'statementOffset' => 38], (new \ZtdQuery\Platform\Postgres\Sql\Cte\HeaderParser())->parseHeader($sql));
    }

    public function testFindAsIndex(): void
    {
        $sql = 'WITH recent AS (SELECT id FROM users) SELECT * FROM recent';
        $stream = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create());
        $tokens = $stream->significantTokens();

        self::assertSame(null, (new \ZtdQuery\Platform\Postgres\Sql\Cte\HeaderParser())->findAsIndex($tokens, 1));
    }

    public function testIsSymbol(): void
    {
        $sql = 'WITH recent AS (SELECT id FROM users) SELECT * FROM recent';
        $stream = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create());
        $tokens = $stream->significantTokens();

        self::assertSame(true, (new \ZtdQuery\Platform\Postgres\Sql\Cte\HeaderParser())->isSymbol($tokens[3], '('));
    }

    public function testIdentifierName(): void
    {
        $sql = 'WITH recent AS (SELECT id FROM users) SELECT * FROM recent';
        $stream = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create());
        $tokens = $stream->significantTokens();

        self::assertSame('recent', (new \ZtdQuery\Platform\Postgres\Sql\Cte\HeaderParser())->identifierName($tokens[1]));
    }
    public function testTopLevelTokensTreatsCteBodiesAsOpaqueSpans(): void
    {
        $tokens = (new \ZtdQuery\Platform\Postgres\Sql\Cte\HeaderParser())->topLevelTokens('WITH x AS (SELECT 1) SELECT * FROM x');
        self::assertSame(['WITH', 'x', 'AS', '(', ')', 'SELECT', '*', 'FROM', 'x'], array_map(static fn (\ZtdQuery\Sql\SqlToken $token): string => $token->text, $tokens));
    }

    public function testBodyStartIndexSkipsMaterializationModifiers(): void
    {
        $sql = 'x AS NOT MATERIALIZED (SELECT 1)';
        $tokens = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create())->significantTokens();
        self::assertSame(4, (new \ZtdQuery\Platform\Postgres\Sql\Cte\HeaderParser())->bodyStartIndex($tokens, 1));
    }
}
