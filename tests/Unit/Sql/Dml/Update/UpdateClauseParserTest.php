<?php

declare(strict_types=1);

namespace Tests\Unit\Sql\Dml\Update;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Sql\Dml\Update\UpdateClauseParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PostgreSqlLexicalMasker::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Lexing\CommentSpan::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Lexing\QuotedSpan::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Lexing\IdentifierDecoder::class)]
final class UpdateClauseParserTest extends TestCase
{
    public function testExtractUpdateAliasReturnsExplicitAlias(): void
    {
        $parser = new \ZtdQuery\Platform\Postgres\Sql\Dml\Update\UpdateClauseParser();
        self::assertSame('u', $parser->extractUpdateAlias('UPDATE users AS u SET id = 1'));
        self::assertSame(null, $parser->extractUpdateAlias('UPDATE users SET id = 1'));
    }

    public function testExtractUpdateSetsKeepsNestedExpressionsTogether(): void
    {
        $parser = new \ZtdQuery\Platform\Postgres\Sql\Dml\Update\UpdateClauseParser();
        self::assertSame(['id' => 'COALESCE(1, 2)', 'name' => 'NULL'], $parser->extractUpdateSets('UPDATE users SET id = COALESCE(1, 2), name = NULL WHERE id = 3'));
        self::assertSame([], $parser->extractUpdateSets('SELECT 1'));
    }
}
