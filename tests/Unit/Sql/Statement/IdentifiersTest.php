<?php

declare(strict_types=1);

namespace Tests\Unit\Sql\Statement;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Sql\Statement\Identifiers::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PostgreSqlLexicalMasker::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Lexing\CommentSpan::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Lexing\QuotedSpan::class)]
final class IdentifiersTest extends TestCase
{
    public function testUnquoteIdentifier(): void
    {
        self::assertSame('User"Name', (new \ZtdQuery\Platform\Postgres\Sql\Statement\Identifiers())->unquoteIdentifier('"User""Name"'));
    }

    public function testStripSchemaPrefix(): void
    {
        self::assertSame('"Users"', (new \ZtdQuery\Platform\Postgres\Sql\Statement\Identifiers())->stripSchemaPrefix('tenant."Users"'));
    }

    public function testParseColumnList(): void
    {
        self::assertSame(['id', 'Name'], (new \ZtdQuery\Platform\Postgres\Sql\Statement\Identifiers())->parseColumnList('id, "Name"'));
    }

    public function testTruncateIdentifierAt(): void
    {
        $sql = 'tenant."Users"';
        $stream = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create());
        $tokens = $stream->significantTokens();

        self::assertSame(['name' => 'tenant', 'next' => 1], (new \ZtdQuery\Platform\Postgres\Sql\Statement\Identifiers())->truncateIdentifierAt($stream, 0));
    }

    public function testExtractInsertTableResolvesSchemaQualifiedTarget(): void
    {
        $parser = new \ZtdQuery\Platform\Postgres\Sql\Statement\Identifiers();
        self::assertSame('users', $parser->extractInsertTable('INSERT /* lead */ INTO public.users (id) VALUES (1)'));
        self::assertSame(null, $parser->extractInsertTable('SELECT 1'));
    }

    public function testExtractInsertColumnsPreservesQuotedColumnNames(): void
    {
        $parser = new \ZtdQuery\Platform\Postgres\Sql\Statement\Identifiers();
        self::assertSame(['id', 'Name'], $parser->extractInsertColumns('INSERT INTO users (id, "Name") VALUES (1, NULL)'));
        self::assertSame([], $parser->extractInsertColumns('INSERT INTO users DEFAULT VALUES'));
    }

    public function testExtractUpdateTableResolvesSchemaQualifiedTarget(): void
    {
        $parser = new \ZtdQuery\Platform\Postgres\Sql\Statement\Identifiers();
        self::assertSame('users', $parser->extractUpdateTable('UPDATE public.users SET id = 1'));
        self::assertSame(null, $parser->extractUpdateTable('SELECT 1'));
    }

    public function testExtractUpdateAliasReturnsExplicitAlias(): void
    {
        $parser = new \ZtdQuery\Platform\Postgres\Sql\Statement\Identifiers();
        self::assertSame('u', $parser->extractUpdateAlias('UPDATE users AS u SET id = 1'));
        self::assertSame(null, $parser->extractUpdateAlias('UPDATE users SET id = 1'));
    }

    public function testExtractUpdateSetsKeepsNestedExpressionsTogether(): void
    {
        $parser = new \ZtdQuery\Platform\Postgres\Sql\Statement\Identifiers();
        self::assertSame(['id' => 'COALESCE(1, 2)', 'name' => 'NULL'], $parser->extractUpdateSets('UPDATE users SET id = COALESCE(1, 2), name = NULL WHERE id = 3'));
        self::assertSame([], $parser->extractUpdateSets('SELECT 1'));
    }

    public function testExtractDeleteTableResolvesSchemaQualifiedTarget(): void
    {
        $parser = new \ZtdQuery\Platform\Postgres\Sql\Statement\Identifiers();
        self::assertSame('users', $parser->extractDeleteTable('DELETE FROM public.users WHERE id = 1'));
        self::assertSame(null, $parser->extractDeleteTable('SELECT 1'));
    }

    public function testExtractDeleteAliasReturnsExplicitAlias(): void
    {
        $parser = new \ZtdQuery\Platform\Postgres\Sql\Statement\Identifiers();
        self::assertSame('u', $parser->extractDeleteAlias('DELETE FROM users AS u WHERE u.id = 1'));
        self::assertSame(null, $parser->extractDeleteAlias('DELETE FROM users WHERE id = 1'));
    }
}
