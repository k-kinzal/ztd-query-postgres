<?php

declare(strict_types=1);

namespace Tests\Unit\Sql\Relation;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Sql\Relation\RelationReference::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::class)]
final class RelationReferenceTest extends TestCase
{
    public function testClosingToken(): void
    {
        $sql = '(SELECT 1)';
        $stream = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create());
        $tokens = $stream->significantTokens();

        self::assertEquals(new \ZtdQuery\Sql\SqlToken(kind: \ZtdQuery\Sql\SqlTokenKind::Symbol, text: ')', offset: 9, depth: 0, bracketDepth: 0), (new \ZtdQuery\Platform\Postgres\Sql\Relation\RelationReference())->closingToken($tokens, 0));
    }

    public function testReferenceAt(): void
    {
        $sql = 'public."Users" u';
        $stream = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create());
        $tokens = $stream->significantTokens();

        self::assertSame(['name' => 'Users', 'start' => 0, 'unqualifiedStart' => 7, 'end' => 14], (new \ZtdQuery\Platform\Postgres\Sql\Relation\RelationReference())->referenceAt($sql, $tokens, 0));
    }

    public function testIdentifierComponentAt(): void
    {
        $sql = 'public."Users" u';
        $stream = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create());
        $tokens = $stream->significantTokens();

        self::assertSame(['Users', 3, 7, 7, 14], (new \ZtdQuery\Platform\Postgres\Sql\Relation\RelationReference())->identifierComponentAt($tokens, 2));
    }

    public function testTokens(): void
    {
        self::assertEquals([new \ZtdQuery\Sql\SqlToken(kind: \ZtdQuery\Sql\SqlTokenKind::Word, text: 'SELECT', offset: 0, depth: 0, bracketDepth: 0), new \ZtdQuery\Sql\SqlToken(kind: \ZtdQuery\Sql\SqlTokenKind::Word, text: 'id', offset: 21, depth: 0, bracketDepth: 0), new \ZtdQuery\Sql\SqlToken(kind: \ZtdQuery\Sql\SqlTokenKind::Word, text: 'FROM', offset: 24, depth: 0, bracketDepth: 0), new \ZtdQuery\Sql\SqlToken(kind: \ZtdQuery\Sql\SqlTokenKind::Word, text: 'users', offset: 29, depth: 0, bracketDepth: 0)], (new \ZtdQuery\Platform\Postgres\Sql\Relation\RelationReference())->tokens('SELECT /* comment */ id FROM users'));
    }
}
