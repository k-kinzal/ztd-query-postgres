<?php

declare(strict_types=1);

namespace Tests\Unit\Sql\Lexing;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Sql\Lexing\IdentifierDecoder::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PostgreSqlLexicalMasker::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Lexing\CommentSpan::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Lexing\QuotedSpan::class)]
final class IdentifierDecoderTest extends TestCase
{
    public function testUnquoteIdentifier(): void
    {
        self::assertSame('User"Name', (new \ZtdQuery\Platform\Postgres\Sql\Lexing\IdentifierDecoder())->unquoteIdentifier('"User""Name"'));
    }

    public function testStripSchemaPrefix(): void
    {
        self::assertSame('"Users"', (new \ZtdQuery\Platform\Postgres\Sql\Lexing\IdentifierDecoder())->stripSchemaPrefix('tenant."Users"'));
    }

    public function testParseColumnList(): void
    {
        self::assertSame(['id', 'Name'], (new \ZtdQuery\Platform\Postgres\Sql\Lexing\IdentifierDecoder())->parseColumnList('id, "Name"'));
    }

    public function testTruncateIdentifierAt(): void
    {
        $sql = 'tenant."Users"';
        $stream = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create());
        $tokens = $stream->significantTokens();

        self::assertSame(['name' => 'tenant', 'next' => 1], (new \ZtdQuery\Platform\Postgres\Sql\Lexing\IdentifierDecoder())->truncateIdentifierAt($stream, 0));
    }
}
