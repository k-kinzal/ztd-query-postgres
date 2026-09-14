<?php

declare(strict_types=1);

namespace Tests\Unit\Sql\Relation;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Sql\Relation\FromClause::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Relation\RelationReference::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::class)]
final class FromClauseTest extends TestCase
{
    public function testReferencesFromClause(): void
    {
        self::assertSame([['name' => 'users', 'start' => 0, 'unqualifiedStart' => 0, 'end' => 5], ['name' => 'orders', 'start' => 13, 'unqualifiedStart' => 13, 'end' => 19]], (new \ZtdQuery\Platform\Postgres\Sql\Relation\FromClause())->referencesFromClause('users u JOIN orders o ON u.id = o.user_id'));
    }

    public function testFindFromEnd(): void
    {
        $sql = 'SELECT * FROM users u JOIN orders o ON u.id = o.user_id WHERE u.id = 1';
        $stream = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create());
        $tokens = $stream->significantTokens();

        self::assertSame(56, (new \ZtdQuery\Platform\Postgres\Sql\Relation\FromClause())->findFromEnd($sql, $tokens, $tokens[2]));
    }

    public function testMatchesKeywordSequence(): void
    {
        $sql = 'LEFT JOIN users';
        $stream = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create());
        $tokens = $stream->significantTokens();

        self::assertSame(true, (new \ZtdQuery\Platform\Postgres\Sql\Relation\FromClause())->matchesKeywordSequence($tokens, 0, ['LEFT', 'JOIN']));
    }
    public function testParenthesizedReferencesRebasesJoinedRelationOffsets(): void
    {
        $sql = '(users JOIN orders ON users.id = orders.user_id)';
        $tokens = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create())->significantTokens();
        $references = (new \ZtdQuery\Platform\Postgres\Sql\Relation\FromClause())->parenthesizedReferences($sql, $tokens, 0, $tokens[0]);
        self::assertSame(['users', 'orders'], array_column($references, 'name'));
        self::assertSame(1, $references[0]['start']);
        self::assertSame(6, $references[0]['end']);
        self::assertSame(12, $references[1]['start']);
        self::assertSame(18, $references[1]['end']);
    }
}
