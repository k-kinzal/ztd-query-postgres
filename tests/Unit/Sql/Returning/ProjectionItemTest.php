<?php

declare(strict_types=1);

namespace Tests\Unit\Sql\Returning;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Sql\Returning\ProjectionItem::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::class)]
final class ProjectionItemTest extends TestCase
{
    public function testParseItem(): void
    {
        self::assertSame(['source' => 'id', 'output' => 'user_id'], (new \ZtdQuery\Platform\Postgres\Sql\Returning\ProjectionItem())->parseItem('users.id AS user_id'));
    }

    public function testAsIndex(): void
    {
        $sql = 'id AS user_id';
        $stream = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create());
        $tokens = $stream->significantTokens();

        self::assertSame(1, (new \ZtdQuery\Platform\Postgres\Sql\Returning\ProjectionItem())->asIndex($tokens));
    }

    public function testIsIdentifierPath(): void
    {
        $sql = 'users.id';
        $stream = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create());
        $tokens = $stream->significantTokens();

        self::assertSame(true, (new \ZtdQuery\Platform\Postgres\Sql\Returning\ProjectionItem())->isIdentifierPath($tokens));
    }

    public function testIdentifierName(): void
    {
        $sql = '"UserId"';
        $stream = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create());
        $tokens = $stream->significantTokens();

        self::assertSame('UserId', (new \ZtdQuery\Platform\Postgres\Sql\Returning\ProjectionItem())->identifierName($tokens[0]));
    }
}
