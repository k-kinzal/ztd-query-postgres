<?php

declare(strict_types=1);

namespace Tests\Unit\Schema\Definition;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Schema\Definition\TableBody::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Lexing\QuotedSpan::class)]
final class TableBodyTest extends TestCase
{
    public function testTableBody(): void
    {
        self::assertSame('id INT, name TEXT', (new \ZtdQuery\Platform\Postgres\Schema\Definition\TableBody())->tableBody('CREATE TABLE users (id INT, name TEXT)'));
    }

    public function testQualifiedIdentifierAt(): void
    {
        $sql = 'public."Users"';
        $stream = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create());
        $tokens = $stream->significantTokens();

        self::assertSame(['name' => 'Users', 'next' => 3], (new \ZtdQuery\Platform\Postgres\Schema\Definition\TableBody())->qualifiedIdentifierAt($stream, $tokens, 0));
    }

    public function testIsSymbol(): void
    {
        $sql = '(id)';
        $stream = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create());
        $tokens = $stream->significantTokens();

        self::assertSame(true, (new \ZtdQuery\Platform\Postgres\Schema\Definition\TableBody())->isSymbol($tokens[0], '('));
    }

    public function testSplitTableBody(): void
    {
        self::assertSame(['id NUMERIC(10,2)', 'name TEXT', 'PRIMARY KEY (id, name)'], (new \ZtdQuery\Platform\Postgres\Schema\Definition\TableBody())->splitTableBody('id NUMERIC(10,2), name TEXT, PRIMARY KEY (id, name)'));
    }
    public function testOpeningParenthesisFollowsTheQualifiedTableName(): void
    {
        $sql = 'CREATE TABLE IF NOT EXISTS public.users (id INT)';
        $tokens = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create())->significantTokens();
        $stream = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create());
        $open = (new \ZtdQuery\Platform\Postgres\Schema\Definition\TableBody())->openingParenthesis($stream, $tokens);
        self::assertNotNull($open);
        self::assertSame('(', $open->text);
        self::assertSame('(id INT)', substr($sql, $open->offset));
    }

    public function testSkipExistenceGuardRequiresTheWholeKeywordSequence(): void
    {
        $sql = 'IF NOT EXISTS users';
        $tokens = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create())->significantTokens();
        $body = new \ZtdQuery\Platform\Postgres\Schema\Definition\TableBody();
        self::assertSame(3, $body->skipExistenceGuard($tokens, 0));
        self::assertNull($body->skipExistenceGuard(array_slice($tokens, 0, 2), 0));
    }
}
