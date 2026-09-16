<?php

declare(strict_types=1);

namespace Tests\Unit\Sql\Dml\Delete;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Sql\Dml\Delete\DeleteClauseParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PostgreSqlLexicalMasker::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Lexing\CommentSpan::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Lexing\QuotedSpan::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Lexing\IdentifierDecoder::class)]
final class DeleteClauseParserTest extends TestCase
{
    public function testExtractDeleteAliasReturnsExplicitAlias(): void
    {
        $parser = new \ZtdQuery\Platform\Postgres\Sql\Dml\Delete\DeleteClauseParser();
        self::assertSame('u', $parser->extractDeleteAlias('DELETE FROM users AS u WHERE u.id = 1'));
        self::assertSame(null, $parser->extractDeleteAlias('DELETE FROM users WHERE id = 1'));
    }
}
