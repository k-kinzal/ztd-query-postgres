<?php

declare(strict_types=1);

namespace Tests\Unit\Sql\Merge;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Sql\Merge\RelationTarget::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::class)]
final class RelationTargetTest extends TestCase
{
    public function testRelationAt(): void
    {
        $sql = 'public."Users" u';
        $tokens = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create())->significantTokens();

        self::assertEquals(['name' => 'Users', 'sql' => 'public."Users"', 'next' => 3, 'last' => new \ZtdQuery\Sql\SqlToken(kind: \ZtdQuery\Sql\SqlTokenKind::QuotedIdentifier, text: '"Users"', offset: 7, depth: 0, bracketDepth: 0)], (new \ZtdQuery\Platform\Postgres\Sql\Merge\RelationTarget())->relationAt($sql, $tokens, 0));

        $sql = 'ONLY users';
        $tokens = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create())->significantTokens();

        self::assertEquals(['name' => 'users', 'sql' => 'users', 'next' => 2, 'last' => new \ZtdQuery\Sql\SqlToken(kind: \ZtdQuery\Sql\SqlTokenKind::Word, text: 'users', offset: 5, depth: 0, bracketDepth: 0)], (new \ZtdQuery\Platform\Postgres\Sql\Merge\RelationTarget())->relationAt($sql, $tokens, 1));

        $sql = '()';
        $tokens = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create())->significantTokens();

        self::assertSame(null, (new \ZtdQuery\Platform\Postgres\Sql\Merge\RelationTarget())->relationAt($sql, $tokens, 0));
    }

    public function testTargetAlias(): void
    {
        $sql = 'AS u';
        $tokens = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create())->significantTokens();

        self::assertSame('u', (new \ZtdQuery\Platform\Postgres\Sql\Merge\RelationTarget())->targetAlias($sql, $tokens, 0, 2, 'users'));

        $sql = '';
        $tokens = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create())->significantTokens();

        self::assertSame('users', (new \ZtdQuery\Platform\Postgres\Sql\Merge\RelationTarget())->targetAlias($sql, $tokens, 0, 0, 'users'));
    }

    public function testIdentifierName(): void
    {
        $sql = '"a""b"';
        $tokens = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create())->significantTokens();

        self::assertSame('a"b', (new \ZtdQuery\Platform\Postgres\Sql\Merge\RelationTarget())->identifierName($tokens[0]));

        $sql = 'users';
        $tokens = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create())->significantTokens();

        self::assertSame('users', (new \ZtdQuery\Platform\Postgres\Sql\Merge\RelationTarget())->identifierName($tokens[0]));

        $sql = '(';
        $tokens = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create())->significantTokens();

        self::assertSame(null, (new \ZtdQuery\Platform\Postgres\Sql\Merge\RelationTarget())->identifierName($tokens[0]));
    }

    public function testIsSymbol(): void
    {
        $sql = '(';
        $tokens = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create())->significantTokens();

        self::assertSame(true, (new \ZtdQuery\Platform\Postgres\Sql\Merge\RelationTarget())->isSymbol($tokens[0], '('));

        $sql = 'word';
        $tokens = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create())->significantTokens();

        self::assertSame(false, (new \ZtdQuery\Platform\Postgres\Sql\Merge\RelationTarget())->isSymbol($tokens[0], '('));
    }
}
