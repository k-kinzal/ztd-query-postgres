<?php

declare(strict_types=1);

namespace Tests\Unit\Sql\Statement;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Sql\Statement\TargetTableParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PostgreSqlLexicalMasker::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Lexing\CommentSpan::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Lexing\QuotedSpan::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Lexing\IdentifierDecoder::class)]
final class TargetTableParserTest extends TestCase
{
    public function testExtractInsertTableResolvesSchemaQualifiedTarget(): void
    {
        $parser = new \ZtdQuery\Platform\Postgres\Sql\Statement\TargetTableParser();
        self::assertSame('users', $parser->extractInsertTable('INSERT /* lead */ INTO public.users (id) VALUES (1)'));
        self::assertSame(null, $parser->extractInsertTable('SELECT 1'));
    }

    public function testExtractUpdateTableResolvesSchemaQualifiedTarget(): void
    {
        $parser = new \ZtdQuery\Platform\Postgres\Sql\Statement\TargetTableParser();
        self::assertSame('users', $parser->extractUpdateTable('UPDATE public.users SET id = 1'));
        self::assertSame(null, $parser->extractUpdateTable('SELECT 1'));
    }

    public function testExtractDeleteTableResolvesSchemaQualifiedTarget(): void
    {
        $parser = new \ZtdQuery\Platform\Postgres\Sql\Statement\TargetTableParser();
        self::assertSame('users', $parser->extractDeleteTable('DELETE FROM public.users WHERE id = 1'));
        self::assertSame(null, $parser->extractDeleteTable('SELECT 1'));
    }
}
