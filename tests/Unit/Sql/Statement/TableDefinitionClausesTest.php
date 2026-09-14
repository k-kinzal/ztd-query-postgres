<?php

declare(strict_types=1);

namespace Tests\Unit\Sql\Statement;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Sql\Statement\TableDefinitionClauses::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Statement\Identifiers::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PostgreSqlLexicalMasker::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Lexing\CommentSpan::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Lexing\QuotedSpan::class)]
final class TableDefinitionClausesTest extends TestCase
{
    public function testExtractTruncateTables(): void
    {
        self::assertSame(['users', 'orders'], (new \ZtdQuery\Platform\Postgres\Sql\Statement\TableDefinitionClauses())->extractTruncateTables('TRUNCATE TABLE ONLY public.users, orders RESTART IDENTITY CASCADE'));
    }

    public function testExtractCreateTableName(): void
    {
        self::assertSame('users', (new \ZtdQuery\Platform\Postgres\Sql\Statement\TableDefinitionClauses())->extractCreateTableName('CREATE TABLE "users" (id INT)'));
    }

    public function testHasIfNotExists(): void
    {
        self::assertSame(true, (new \ZtdQuery\Platform\Postgres\Sql\Statement\TableDefinitionClauses())->hasIfNotExists('CREATE TABLE IF NOT EXISTS users (id INT)'));
    }

    public function testHasCreateTableAsSelect(): void
    {
        self::assertSame(true, (new \ZtdQuery\Platform\Postgres\Sql\Statement\TableDefinitionClauses())->hasCreateTableAsSelect('CREATE TABLE users AS SELECT 1 AS id'));
    }

    public function testExtractCreateTableSelectSql(): void
    {
        self::assertSame('SELECT 1 AS id', (new \ZtdQuery\Platform\Postgres\Sql\Statement\TableDefinitionClauses())->extractCreateTableSelectSql('CREATE TABLE users AS SELECT 1 AS id'));
    }

    public function testHasCreateTableLike(): void
    {
        self::assertSame(true, (new \ZtdQuery\Platform\Postgres\Sql\Statement\TableDefinitionClauses())->hasCreateTableLike('CREATE TABLE users (LIKE source INCLUDING ALL)'));
    }

    public function testExtractCreateTableLikeSource(): void
    {
        self::assertSame('source', (new \ZtdQuery\Platform\Postgres\Sql\Statement\TableDefinitionClauses())->extractCreateTableLikeSource('CREATE TABLE users (LIKE source INCLUDING ALL)'));
    }

    public function testExtractDropTableName(): void
    {
        self::assertSame('users', (new \ZtdQuery\Platform\Postgres\Sql\Statement\TableDefinitionClauses())->extractDropTableName('DROP TABLE IF EXISTS users'));
    }

    public function testHasDropTableIfExists(): void
    {
        self::assertSame(true, (new \ZtdQuery\Platform\Postgres\Sql\Statement\TableDefinitionClauses())->hasDropTableIfExists('DROP TABLE IF EXISTS users'));
    }

    public function testExtractAlterTableName(): void
    {
        self::assertSame('users', (new \ZtdQuery\Platform\Postgres\Sql\Statement\TableDefinitionClauses())->extractAlterTableName('ALTER TABLE users ADD COLUMN name TEXT'));
    }
}
