<?php

declare(strict_types=1);

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use ZtdQuery\Platform\Postgres\PgSqlParser;
use PHPUnit\Framework\Attributes\CoversClass;

#[CoversClass(PgSqlParser::class)]
final class PgSqlParserTest extends TestCase
{
    public function testClassifySelect(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('SELECT', $parser->classifyStatement('SELECT * FROM users'));
    }

    public function testClassifyInsert(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('INSERT', $parser->classifyStatement("INSERT INTO users (id) VALUES (1)"));
    }

    public function testClassifyUpdate(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('UPDATE', $parser->classifyStatement("UPDATE users SET name = 'x'"));
    }

    public function testClassifyDelete(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('DELETE', $parser->classifyStatement('DELETE FROM users WHERE id = 1'));
    }

    public function testClassifyTruncate(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('TRUNCATE', $parser->classifyStatement('TRUNCATE TABLE users'));
    }

    public function testClassifyCreateTable(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('CREATE_TABLE', $parser->classifyStatement('CREATE TABLE users (id INTEGER)'));
    }

    public function testClassifyDropTable(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('DROP_TABLE', $parser->classifyStatement('DROP TABLE users'));
    }

    public function testClassifyAlterTable(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('ALTER_TABLE', $parser->classifyStatement('ALTER TABLE users ADD COLUMN name TEXT'));
    }

    public function testClassifyWithSelect(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('SELECT', $parser->classifyStatement('WITH cte AS (SELECT 1) SELECT * FROM cte'));
    }

    public function testClassifyWithInsert(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('INSERT', $parser->classifyStatement('WITH vals AS (SELECT 1) INSERT INTO users SELECT * FROM vals'));
    }

    public function testClassifyTcl(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('TCL', $parser->classifyStatement('BEGIN'));
        self::assertSame('TCL', $parser->classifyStatement('COMMIT'));
        self::assertSame('TCL', $parser->classifyStatement('ROLLBACK'));
    }

    public function testClassifyUnknownReturnsNull(): void
    {
        $parser = new PgSqlParser();
        self::assertNull($parser->classifyStatement('CREATE DATABASE test'));
    }

    public function testClassifyWithLeadingComments(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('SELECT', $parser->classifyStatement('-- comment
SELECT * FROM users'));
    }

    public function testSplitSingleStatement(): void
    {
        $parser = new PgSqlParser();
        $result = $parser->splitStatements('SELECT 1');
        self::assertCount(1, $result);
        self::assertSame('SELECT 1', $result[0]);
    }

    public function testSplitMultipleStatements(): void
    {
        $parser = new PgSqlParser();
        $result = $parser->splitStatements('SELECT 1; SELECT 2');
        self::assertCount(2, $result);
    }

    public function testSplitHandlesStringLiterals(): void
    {
        $parser = new PgSqlParser();
        $result = $parser->splitStatements("SELECT 'a;b'");
        self::assertCount(1, $result);
    }

    public function testSplitHandlesDollarQuoting(): void
    {
        $parser = new PgSqlParser();
        $result = $parser->splitStatements('SELECT $$hello;world$$');
        self::assertCount(1, $result);
    }

    public function testExtractInsertTable(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('users', $parser->extractInsertTable("INSERT INTO users (id) VALUES (1)"));
    }

    public function testExtractInsertTableQuoted(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('users', $parser->extractInsertTable('INSERT INTO "users" (id) VALUES (1)'));
    }

    public function testExtractInsertTableSchemaQualified(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('users', $parser->extractInsertTable('INSERT INTO public.users (id) VALUES (1)'));
    }

    public function testExtractInsertColumns(): void
    {
        $parser = new PgSqlParser();
        $cols = $parser->extractInsertColumns("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'a@b.com')");
        self::assertSame(['id', 'name', 'email'], $cols);
    }

    public function testExtractInsertColumnsQuoted(): void
    {
        $parser = new PgSqlParser();
        $cols = $parser->extractInsertColumns('INSERT INTO users ("id", "name") VALUES (1, \'x\')');
        self::assertSame(['id', 'name'], $cols);
    }

    public function testExtractInsertValuesSingle(): void
    {
        $parser = new PgSqlParser();
        $rows = $parser->extractInsertValues("INSERT INTO users (id, name) VALUES (1, 'Alice')");
        self::assertCount(1, $rows);
        self::assertSame(['1', "'Alice'"], $rows[0]);
    }

    public function testExtractInsertValuesMultiple(): void
    {
        $parser = new PgSqlParser();
        $rows = $parser->extractInsertValues("INSERT INTO users (id) VALUES (1), (2), (3)");
        self::assertCount(3, $rows);
    }

    public function testHasOnConflict(): void
    {
        $parser = new PgSqlParser();
        self::assertTrue($parser->hasOnConflict("INSERT INTO users (id) VALUES (1) ON CONFLICT (id) DO NOTHING"));
    }

    public function testHasOnConflictFalse(): void
    {
        $parser = new PgSqlParser();
        self::assertFalse($parser->hasOnConflict("INSERT INTO users (id) VALUES (1)"));
    }

    public function testExtractOnConflictUpdateColumns(): void
    {
        $parser = new PgSqlParser();
        $result = $parser->extractOnConflictUpdateColumns(
            "INSERT INTO users (id, name) VALUES (1, 'Alice') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name"
        );
        self::assertSame(['name'], $result['columns']);
        self::assertSame('EXCLUDED.name', $result['values']['name']);
    }

    public function testExtractUpdateTable(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('users', $parser->extractUpdateTable("UPDATE users SET name = 'x'"));
    }

    public function testExtractUpdateTableSchemaQualified(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('users', $parser->extractUpdateTable("UPDATE public.users SET name = 'x'"));
    }

    public function testExtractUpdateSets(): void
    {
        $parser = new PgSqlParser();
        $sets = $parser->extractUpdateSets("UPDATE users SET name = 'Bob', age = 30 WHERE id = 1");
        self::assertSame("'Bob'", $sets['name']);
        self::assertSame('30', $sets['age']);
    }

    public function testExtractDeleteTable(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('users', $parser->extractDeleteTable('DELETE FROM users WHERE id = 1'));
    }

    public function testExtractDeleteTableSchemaQualified(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('users', $parser->extractDeleteTable('DELETE FROM public.users WHERE id = 1'));
    }

    public function testExtractTruncateTable(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('users', $parser->extractTruncateTable('TRUNCATE TABLE users'));
    }

    public function testExtractTruncateTableWithCascade(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('users', $parser->extractTruncateTable('TRUNCATE TABLE users CASCADE'));
    }

    public function testExtractCreateTableName(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('users', $parser->extractCreateTableName('CREATE TABLE users (id INTEGER)'));
    }

    public function testExtractCreateTableNameIfNotExists(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('users', $parser->extractCreateTableName('CREATE TABLE IF NOT EXISTS users (id INTEGER)'));
    }

    public function testHasIfNotExists(): void
    {
        $parser = new PgSqlParser();
        self::assertTrue($parser->hasIfNotExists('CREATE TABLE IF NOT EXISTS users (id INTEGER)'));
        self::assertFalse($parser->hasIfNotExists('CREATE TABLE users (id INTEGER)'));
    }

    public function testHasCreateTableAsSelect(): void
    {
        $parser = new PgSqlParser();
        self::assertTrue($parser->hasCreateTableAsSelect('CREATE TABLE users AS SELECT * FROM old_users'));
        self::assertFalse($parser->hasCreateTableAsSelect('CREATE TABLE users (id INTEGER)'));
    }

    public function testExtractDropTableName(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('users', $parser->extractDropTableName('DROP TABLE users'));
    }

    public function testHasDropTableIfExists(): void
    {
        $parser = new PgSqlParser();
        self::assertTrue($parser->hasDropTableIfExists('DROP TABLE IF EXISTS users'));
        self::assertFalse($parser->hasDropTableIfExists('DROP TABLE users'));
    }

    public function testExtractAlterTableName(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('users', $parser->extractAlterTableName('ALTER TABLE users ADD COLUMN email TEXT'));
    }

    public function testStripSchemaPrefix(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('users', $parser->stripSchemaPrefix('public.users'));
        self::assertSame('"users"', $parser->stripSchemaPrefix('"public"."users"'));
        self::assertSame('users', $parser->stripSchemaPrefix('users'));
    }

    public function testUnquoteIdentifier(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('users', $parser->unquoteIdentifier('"users"'));
        self::assertSame('users', $parser->unquoteIdentifier('users'));
    }

    public function testExtractSelectTableNames(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames('SELECT * FROM users JOIN orders ON users.id = orders.user_id');
        self::assertContains('users', $tables);
        self::assertContains('orders', $tables);
    }

    public function testExtractWhereClause(): void
    {
        $parser = new PgSqlParser();
        $where = $parser->extractWhereClause('DELETE FROM users WHERE id = 1');
        self::assertSame('id = 1', $where);
    }

    public function testClassifyWithLowercase(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('SELECT', $parser->classifyStatement('with cte as (select 1) select * from cte'));
    }

    public function testClassifySelectNotMatchedByWith(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('SELECT', $parser->classifyStatement('SELECT * FROM with_table'));
    }

    public function testClassifyWithUpdate(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('UPDATE', $parser->classifyStatement('WITH cte AS (SELECT 1) UPDATE users SET x = 1'));
    }

    public function testClassifyWithDelete(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('DELETE', $parser->classifyStatement('WITH cte AS (SELECT 1) DELETE FROM users'));
    }

    public function testClassifyWithNoTerminalKeyword(): void
    {
        $parser = new PgSqlParser();
        self::assertNull($parser->classifyStatement('WITH cte AS (SELECT 1)'));
    }

    public function testClassifyWithBlockComment(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('SELECT', $parser->classifyStatement("/* comment */\nSELECT 1"));
    }

    public function testClassifyWithBlockCommentNoNewline(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('SELECT', $parser->classifyStatement('/* comment */ SELECT 1'));
    }

    public function testClassifyCreateTempTable(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('CREATE_TABLE', $parser->classifyStatement('CREATE TEMPORARY TABLE t (id INT)'));
        self::assertSame('CREATE_TABLE', $parser->classifyStatement('CREATE TEMP TABLE t (id INT)'));
        self::assertSame('CREATE_TABLE', $parser->classifyStatement('CREATE UNLOGGED TABLE t (id INT)'));
    }

    public function testClassifyTclVariants(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('TCL', $parser->classifyStatement('START TRANSACTION'));
        self::assertSame('TCL', $parser->classifyStatement('SAVEPOINT sp1'));
        self::assertSame('TCL', $parser->classifyStatement('RELEASE SAVEPOINT sp1'));
        self::assertSame('TCL', $parser->classifyStatement('SET TRANSACTION ISOLATION LEVEL SERIALIZABLE'));
    }

    public function testSplitMultipleExactContent(): void
    {
        $parser = new PgSqlParser();
        $result = $parser->splitStatements('SELECT 1; SELECT 2');
        self::assertSame(['SELECT 1', 'SELECT 2'], $result);
    }

    public function testSplitEmptyInput(): void
    {
        $parser = new PgSqlParser();
        self::assertSame([], $parser->splitStatements(''));
    }

    public function testSplitWhitespaceOnly(): void
    {
        $parser = new PgSqlParser();
        self::assertSame([], $parser->splitStatements('   '));
    }

    public function testSplitTrailingSemicolon(): void
    {
        $parser = new PgSqlParser();
        $result = $parser->splitStatements('SELECT 1;');
        self::assertSame(['SELECT 1'], $result);
    }

    public function testSplitEscapedSingleQuote(): void
    {
        $parser = new PgSqlParser();
        $result = $parser->splitStatements("SELECT 'it''s'; SELECT 2");
        self::assertSame(2, count($result));
        self::assertStringContainsString("it''s", $result[0]);
        self::assertSame('SELECT 2', $result[1]);
    }

    public function testSplitDoubleQuotedIdentifier(): void
    {
        $parser = new PgSqlParser();
        $result = $parser->splitStatements('SELECT "col;name" FROM t; SELECT 2');
        self::assertSame(2, count($result));
        self::assertStringContainsString('"col;name"', $result[0]);
    }

    public function testSplitEscapedDoubleQuote(): void
    {
        $parser = new PgSqlParser();
        $result = $parser->splitStatements('SELECT "a""b" FROM t; SELECT 2');
        self::assertSame(2, count($result));
        self::assertStringContainsString('"a""b"', $result[0]);
    }

    public function testSplitLineComment(): void
    {
        $parser = new PgSqlParser();
        $result = $parser->splitStatements("SELECT 1 -- comment;\n; SELECT 2");
        self::assertSame(2, count($result));
    }

    public function testSplitBlockComment(): void
    {
        $parser = new PgSqlParser();
        $result = $parser->splitStatements('SELECT /* ; */ 1; SELECT 2');
        self::assertSame(2, count($result));
        self::assertStringContainsString('/* ; */', $result[0]);
    }

    public function testSplitDollarQuoteWithTag(): void
    {
        $parser = new PgSqlParser();
        $result = $parser->splitStatements('SELECT $tag$hello;world$tag$; SELECT 2');
        self::assertSame(2, count($result));
        self::assertStringContainsString('hello;world', $result[0]);
    }

    public function testExtractInsertTableOnly(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('t', $parser->extractInsertTable('INSERT INTO ONLY t (id) VALUES (1)'));
    }

    public function testExtractInsertTableWithAlias(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('users', $parser->extractInsertTable('INSERT INTO users AS u (id) VALUES (1)'));
    }

    public function testExtractInsertTableReturnsNullOnInvalid(): void
    {
        $parser = new PgSqlParser();
        self::assertNull($parser->extractInsertTable('SELECT 1'));
    }

    public function testExtractInsertColumnsEmpty(): void
    {
        $parser = new PgSqlParser();
        self::assertSame([], $parser->extractInsertColumns('INSERT INTO users VALUES (1)'));
    }

    public function testExtractInsertColumnsWithSelect(): void
    {
        $parser = new PgSqlParser();
        $cols = $parser->extractInsertColumns('INSERT INTO users (id, name) SELECT id, name FROM old');
        self::assertSame(['id', 'name'], $cols);
    }

    public function testExtractInsertValuesEmpty(): void
    {
        $parser = new PgSqlParser();
        self::assertSame([], $parser->extractInsertValues('INSERT INTO users SELECT * FROM old'));
    }

    public function testExtractInsertValuesWithSubquery(): void
    {
        $parser = new PgSqlParser();
        $rows = $parser->extractInsertValues("INSERT INTO t (id, v) VALUES (1, (SELECT max(id) FROM t2))");
        self::assertSame(1, count($rows));
        self::assertSame('1', $rows[0][0]);
        self::assertSame('(SELECT max(id) FROM t2)', $rows[0][1]);
    }

    public function testExtractInsertValuesWithEscapedQuote(): void
    {
        $parser = new PgSqlParser();
        $rows = $parser->extractInsertValues("INSERT INTO t (v) VALUES ('it''s')");
        self::assertSame(1, count($rows));
        self::assertSame("'it''s'", $rows[0][0]);
    }

    public function testExtractOnConflictUpdateColumnsNoConflict(): void
    {
        $parser = new PgSqlParser();
        $result = $parser->extractOnConflictUpdateColumns("INSERT INTO t (id) VALUES (1)");
        self::assertSame([], $result['columns']);
        self::assertSame([], $result['values']);
    }

    public function testExtractOnConflictUpdateColumnsMultiple(): void
    {
        $parser = new PgSqlParser();
        $result = $parser->extractOnConflictUpdateColumns(
            "INSERT INTO t (id, a, b) VALUES (1, 2, 3) ON CONFLICT (id) DO UPDATE SET a = EXCLUDED.a, b = EXCLUDED.b"
        );
        self::assertSame(['a', 'b'], $result['columns']);
        self::assertSame('EXCLUDED.a', $result['values']['a']);
        self::assertSame('EXCLUDED.b', $result['values']['b']);
    }

    public function testExtractOnConflictWithWhereClause(): void
    {
        $parser = new PgSqlParser();
        $result = $parser->extractOnConflictUpdateColumns(
            "INSERT INTO t (id, v) VALUES (1, 2) ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v WHERE t.v < EXCLUDED.v"
        );
        self::assertSame(['v'], $result['columns']);
        self::assertSame('EXCLUDED.v', $result['values']['v']);
    }

    public function testExtractOnConflictQuotedColumn(): void
    {
        $parser = new PgSqlParser();
        $result = $parser->extractOnConflictUpdateColumns(
            'INSERT INTO t (id, "Name") VALUES (1, \'x\') ON CONFLICT (id) DO UPDATE SET "Name" = EXCLUDED."Name"'
        );
        self::assertSame(['Name'], $result['columns']);
    }

    public function testHasInsertSelect(): void
    {
        $parser = new PgSqlParser();
        self::assertTrue($parser->hasInsertSelect('INSERT INTO t SELECT * FROM old'));
        self::assertFalse($parser->hasInsertSelect("INSERT INTO t VALUES (1)"));
    }

    public function testHasInsertSelectWithValueLookingLikeSelect(): void
    {
        $parser = new PgSqlParser();
        self::assertFalse($parser->hasInsertSelect("INSERT INTO t (v) VALUES ('SELECT')"));
    }

    public function testExtractInsertSelectSql(): void
    {
        $parser = new PgSqlParser();
        $sql = $parser->extractInsertSelectSql('INSERT INTO t (id) SELECT id FROM old WHERE id > 5');
        self::assertSame('SELECT id FROM old WHERE id > 5', $sql);
    }

    public function testExtractInsertSelectSqlReturnsNull(): void
    {
        $parser = new PgSqlParser();
        self::assertNull($parser->extractInsertSelectSql('INSERT INTO t (id) VALUES (1)'));
    }

    public function testExtractUpdateTableQuoted(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('My Table', $parser->extractUpdateTable('UPDATE "My Table" SET x = 1'));
    }

    public function testExtractUpdateTableOnly(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('t', $parser->extractUpdateTable('UPDATE ONLY t SET x = 1'));
    }

    public function testExtractUpdateTableReturnsNull(): void
    {
        $parser = new PgSqlParser();
        self::assertNull($parser->extractUpdateTable('SELECT 1'));
    }

    public function testExtractUpdateAlias(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('u', $parser->extractUpdateAlias('UPDATE users u SET name = \'x\' WHERE id = 1'));
    }

    public function testExtractUpdateAliasAs(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('u', $parser->extractUpdateAlias('UPDATE users AS u SET name = \'x\''));
    }

    public function testExtractUpdateAliasReturnsNull(): void
    {
        $parser = new PgSqlParser();
        self::assertNull($parser->extractUpdateAlias('UPDATE users SET name = \'x\''));
    }

    public function testExtractUpdateSetsWithFrom(): void
    {
        $parser = new PgSqlParser();
        $sets = $parser->extractUpdateSets("UPDATE users SET name = o.name FROM other o WHERE users.id = o.id");
        self::assertSame(['name' => 'o.name'], $sets);
    }

    public function testExtractUpdateSetsReturning(): void
    {
        $parser = new PgSqlParser();
        $sets = $parser->extractUpdateSets("UPDATE users SET name = 'x' RETURNING *");
        self::assertSame(['name' => "'x'"], $sets);
    }

    public function testExtractUpdateSetsEmpty(): void
    {
        $parser = new PgSqlParser();
        self::assertSame([], $parser->extractUpdateSets('SELECT 1'));
    }

    public function testExtractUpdateSetsQuotedColumn(): void
    {
        $parser = new PgSqlParser();
        $sets = $parser->extractUpdateSets('UPDATE t SET "Column" = 1 WHERE id = 1');
        self::assertArrayHasKey('Column', $sets);
        self::assertSame('1', $sets['Column']);
    }

    public function testExtractWhereClauseReturnsNull(): void
    {
        $parser = new PgSqlParser();
        self::assertNull($parser->extractWhereClause('DELETE FROM users'));
    }

    public function testExtractWhereClauseWithReturning(): void
    {
        $parser = new PgSqlParser();
        $where = $parser->extractWhereClause('DELETE FROM users WHERE id = 1 RETURNING *');
        self::assertSame('id = 1', $where);
    }

    public function testExtractWhereClauseWithStringLiteral(): void
    {
        $parser = new PgSqlParser();
        $where = $parser->extractWhereClause("DELETE FROM users WHERE name = 'WHERE test'");
        self::assertNotNull($where);
        self::assertStringContainsString("'WHERE test'", $where);
    }

    public function testExtractWhereClauseWithOrderBy(): void
    {
        $parser = new PgSqlParser();
        $where = $parser->extractWhereClause("DELETE FROM users WHERE active = true ORDER BY id");
        self::assertNotNull($where);
        self::assertStringNotContainsString('ORDER', $where);
    }

    public function testExtractWhereClauseWithLimit(): void
    {
        $parser = new PgSqlParser();
        $where = $parser->extractWhereClause("DELETE FROM users WHERE active = true LIMIT 10");
        self::assertNotNull($where);
        self::assertStringNotContainsString('LIMIT', $where);
    }

    public function testExtractUpdateFromClause(): void
    {
        $parser = new PgSqlParser();
        $from = $parser->extractUpdateFromClause("UPDATE users SET name = o.name FROM other o WHERE users.id = o.id");
        self::assertSame('other o', $from);
    }

    public function testExtractUpdateFromClauseReturnsNull(): void
    {
        $parser = new PgSqlParser();
        self::assertNull($parser->extractUpdateFromClause("UPDATE users SET name = 'x' WHERE id = 1"));
    }

    public function testExtractDeleteTableQuoted(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('users', $parser->extractDeleteTable('DELETE FROM "users" WHERE id = 1'));
    }

    public function testExtractDeleteTableOnly(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('t', $parser->extractDeleteTable('DELETE FROM ONLY t WHERE id = 1'));
    }

    public function testExtractDeleteTableReturnsNull(): void
    {
        $parser = new PgSqlParser();
        self::assertNull($parser->extractDeleteTable('SELECT 1'));
    }

    public function testExtractDeleteAlias(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('u', $parser->extractDeleteAlias('DELETE FROM users u WHERE u.id = 1'));
    }

    public function testExtractDeleteAliasAs(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('u', $parser->extractDeleteAlias('DELETE FROM users AS u WHERE u.id = 1'));
    }

    public function testExtractDeleteAliasReturnsNull(): void
    {
        $parser = new PgSqlParser();
        self::assertNull($parser->extractDeleteAlias('DELETE FROM users WHERE id = 1'));
    }

    public function testExtractDeleteUsingClause(): void
    {
        $parser = new PgSqlParser();
        $using = $parser->extractDeleteUsingClause('DELETE FROM users USING orders WHERE users.id = orders.user_id');
        self::assertSame('orders', $using);
    }

    public function testExtractDeleteUsingClauseReturnsNull(): void
    {
        $parser = new PgSqlParser();
        self::assertNull($parser->extractDeleteUsingClause('DELETE FROM users WHERE id = 1'));
    }

    public function testExtractTruncateTableSchemaQualified(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('users', $parser->extractTruncateTable('TRUNCATE TABLE public.users'));
    }

    public function testExtractTruncateTableOnly(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('t', $parser->extractTruncateTable('TRUNCATE ONLY t'));
    }

    public function testExtractTruncateTableReturnsNull(): void
    {
        $parser = new PgSqlParser();
        self::assertNull($parser->extractTruncateTable('SELECT 1'));
    }

    public function testExtractCreateTableNameSchemaQualified(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('users', $parser->extractCreateTableName('CREATE TABLE public.users (id INT)'));
    }

    public function testExtractCreateTableNameTemp(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('t', $parser->extractCreateTableName('CREATE TEMPORARY TABLE t (id INT)'));
        self::assertSame('t', $parser->extractCreateTableName('CREATE TEMP TABLE t (id INT)'));
    }

    public function testExtractCreateTableNameReturnsNull(): void
    {
        $parser = new PgSqlParser();
        self::assertNull($parser->extractCreateTableName('SELECT 1'));
    }

    public function testExtractCreateTableSelectSql(): void
    {
        $parser = new PgSqlParser();
        $sql = $parser->extractCreateTableSelectSql('CREATE TABLE t AS SELECT id FROM old');
        self::assertSame('SELECT id FROM old', $sql);
    }

    public function testExtractCreateTableSelectSqlReturnsNull(): void
    {
        $parser = new PgSqlParser();
        self::assertNull($parser->extractCreateTableSelectSql('CREATE TABLE t (id INT)'));
    }

    public function testHasCreateTableLike(): void
    {
        $parser = new PgSqlParser();
        self::assertTrue($parser->hasCreateTableLike('CREATE TABLE t (LIKE old_t)'));
        self::assertFalse($parser->hasCreateTableLike('CREATE TABLE t (id INT)'));
    }

    public function testExtractCreateTableLikeSource(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('old_t', $parser->extractCreateTableLikeSource('CREATE TABLE t (LIKE old_t)'));
    }

    public function testExtractCreateTableLikeSourceSchemaQualified(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('old_t', $parser->extractCreateTableLikeSource('CREATE TABLE t (LIKE public.old_t)'));
    }

    public function testExtractCreateTableLikeSourceReturnsNull(): void
    {
        $parser = new PgSqlParser();
        self::assertNull($parser->extractCreateTableLikeSource('CREATE TABLE t (id INT)'));
    }

    public function testExtractDropTableNameIfExists(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('users', $parser->extractDropTableName('DROP TABLE IF EXISTS users'));
    }

    public function testExtractDropTableNameSchemaQualified(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('users', $parser->extractDropTableName('DROP TABLE public.users'));
    }

    public function testExtractDropTableNameReturnsNull(): void
    {
        $parser = new PgSqlParser();
        self::assertNull($parser->extractDropTableName('SELECT 1'));
    }

    public function testExtractAlterTableNameIfExists(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('users', $parser->extractAlterTableName('ALTER TABLE IF EXISTS users ADD COLUMN x INT'));
    }

    public function testExtractAlterTableNameOnly(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('users', $parser->extractAlterTableName('ALTER TABLE ONLY users ADD COLUMN x INT'));
    }

    public function testExtractAlterTableNameReturnsNull(): void
    {
        $parser = new PgSqlParser();
        self::assertNull($parser->extractAlterTableName('SELECT 1'));
    }

    public function testUnquoteIdentifierEscapedQuotes(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('a"b', $parser->unquoteIdentifier('"a""b"'));
    }

    public function testExtractSelectTableNamesSubquery(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames('SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)');
        self::assertContains('users', $tables);
    }

    public function testExtractSelectTableNamesMultipleFrom(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames('SELECT * FROM users, orders WHERE users.id = orders.user_id');
        self::assertContains('users', $tables);
        self::assertContains('orders', $tables);
    }

    public function testExtractSelectTableNamesNoFrom(): void
    {
        $parser = new PgSqlParser();
        self::assertSame([], $parser->extractSelectTableNames('SELECT 1'));
    }

    public function testExtractSelectTableNamesSchemaQualified(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames('SELECT * FROM public.users');
        self::assertContains('users', $tables);
    }

    public function testExtractSelectTableNamesDeduplication(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames('SELECT * FROM users JOIN users ON 1=1');
        self::assertSame(['users'], $tables);
    }

    public function testClassifyWithNestedParens(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('SELECT', $parser->classifyStatement('WITH cte AS (SELECT (1+2)) SELECT * FROM cte'));
    }

    public function testClassifyLineCommentNoNewline(): void
    {
        $parser = new PgSqlParser();
        self::assertNull($parser->classifyStatement('-- only a comment'));
    }

    public function testClassifyBlockCommentOnlyNoClose(): void
    {
        $parser = new PgSqlParser();
        self::assertNull($parser->classifyStatement('/* unclosed comment'));
    }

    public function testStripSchemaPrefixQuotedBoth(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('"My Table"', $parser->stripSchemaPrefix('"myschema"."My Table"'));
    }

    public function testExtractInsertTableQuotedSchemaQualified(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('users', $parser->extractInsertTable('INSERT INTO public."users" (id) VALUES (1)'));
    }

    public function testExtractUpdateSetsWithSubquery(): void
    {
        $parser = new PgSqlParser();
        $sets = $parser->extractUpdateSets("UPDATE t SET v = (SELECT max(id)) WHERE id = 1");
        self::assertArrayHasKey('v', $sets);
        self::assertSame('(SELECT max(id))', $sets['v']);
    }

    public function testExtractDeleteAliasUsing(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('u', $parser->extractDeleteAlias('DELETE FROM users AS u USING orders WHERE u.id = orders.user_id'));
    }

    public function testExtractDeleteAliasReturning(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('u', $parser->extractDeleteAlias('DELETE FROM users u RETURNING *'));
    }

    public function testExtractDeleteUsingWithReturning(): void
    {
        $parser = new PgSqlParser();
        $using = $parser->extractDeleteUsingClause('DELETE FROM users USING orders WHERE 1=1 RETURNING *');
        self::assertSame('orders', $using);
    }

    public function testExtractTruncateTableNoTableKeyword(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('users', $parser->extractTruncateTable('TRUNCATE users'));
    }

    public function testExtractUpdateFromClauseWithReturning(): void
    {
        $parser = new PgSqlParser();
        $from = $parser->extractUpdateFromClause("UPDATE users SET name = o.name FROM other o WHERE 1=1 RETURNING *");
        self::assertSame('other o', $from);
    }

    public function testExtractInsertValuesWithWhitespace(): void
    {
        $parser = new PgSqlParser();
        $rows = $parser->extractInsertValues("INSERT INTO t (id) VALUES (1) , (2)");
        self::assertSame(2, count($rows));
        self::assertSame(['1'], $rows[0]);
        self::assertSame(['2'], $rows[1]);
    }

    public function testSplitDollarQuoteEmpty(): void
    {
        $parser = new PgSqlParser();
        $result = $parser->splitStatements('SELECT $$;$$; SELECT 2');
        self::assertSame(2, count($result));
    }

    public function testExtractSelectTableNamesWithAlias(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames('SELECT * FROM users u WHERE u.id = 1');
        self::assertContains('users', $tables);
    }

    public function testExtractSelectTableNamesLeftJoin(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames('SELECT * FROM users LEFT JOIN orders ON users.id = orders.uid');
        self::assertContains('users', $tables);
        self::assertContains('orders', $tables);
    }

    public function testClassifyWithQuotedIdentifier(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('SELECT', $parser->classifyStatement('WITH "my cte" AS (SELECT 1) SELECT * FROM "my cte"'));
    }

    public function testExtractOnConflictDoNothing(): void
    {
        $parser = new PgSqlParser();
        $result = $parser->extractOnConflictUpdateColumns(
            "INSERT INTO t (id) VALUES (1) ON CONFLICT (id) DO NOTHING"
        );
        self::assertSame([], $result['columns']);
    }

    public function testExtractInsertColumnsWithDefault(): void
    {
        $parser = new PgSqlParser();
        $cols = $parser->extractInsertColumns('INSERT INTO t (id) DEFAULT VALUES');
        self::assertSame(['id'], $cols);
    }

    public function testClassifySelectLowercase(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('SELECT', $parser->classifyStatement('select * from users'));
    }

    public function testClassifyInsertLowercase(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('INSERT', $parser->classifyStatement('insert into users (id) values (1)'));
    }

    public function testClassifyUpdateLowercase(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('UPDATE', $parser->classifyStatement("update users set name = 'x'"));
    }

    public function testClassifyDeleteLowercase(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('DELETE', $parser->classifyStatement('delete from users where id = 1'));
    }

    public function testClassifyTruncateLowercase(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('TRUNCATE', $parser->classifyStatement('truncate table users'));
    }

    public function testClassifyCreateTableLowercase(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('CREATE_TABLE', $parser->classifyStatement('create table users (id int)'));
    }

    public function testClassifyDropTableLowercase(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('DROP_TABLE', $parser->classifyStatement('drop table users'));
    }

    public function testClassifyAlterTableLowercase(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('ALTER_TABLE', $parser->classifyStatement('alter table users add column x int'));
    }

    public function testClassifyTclLowercase(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('TCL', $parser->classifyStatement('begin'));
        self::assertSame('TCL', $parser->classifyStatement('commit'));
        self::assertSame('TCL', $parser->classifyStatement('rollback'));
        self::assertSame('TCL', $parser->classifyStatement('start transaction'));
        self::assertSame('TCL', $parser->classifyStatement('savepoint sp1'));
        self::assertSame('TCL', $parser->classifyStatement('release savepoint sp1'));
        self::assertSame('TCL', $parser->classifyStatement('set transaction read only'));
    }

    public function testExtractInsertTableLowercase(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('users', $parser->extractInsertTable('insert into users (id) values (1)'));
    }

    public function testExtractInsertColumnsLowercase(): void
    {
        $parser = new PgSqlParser();
        $cols = $parser->extractInsertColumns("insert into users (id, name) values (1, 'x')");
        self::assertSame(['id', 'name'], $cols);
    }

    public function testExtractInsertValuesLowercase(): void
    {
        $parser = new PgSqlParser();
        $rows = $parser->extractInsertValues("insert into t (id) values (1)");
        self::assertSame(1, count($rows));
        self::assertSame(['1'], $rows[0]);
    }

    public function testHasOnConflictLowercase(): void
    {
        $parser = new PgSqlParser();
        self::assertTrue($parser->hasOnConflict('insert into t (id) values (1) on conflict (id) do nothing'));
    }

    public function testExtractOnConflictLowercase(): void
    {
        $parser = new PgSqlParser();
        $result = $parser->extractOnConflictUpdateColumns(
            "insert into t (id, v) values (1, 2) on conflict (id) do update set v = excluded.v"
        );
        self::assertSame(['v'], $result['columns']);
    }

    public function testHasInsertSelectLowercase(): void
    {
        $parser = new PgSqlParser();
        self::assertTrue($parser->hasInsertSelect('insert into t select * from old'));
    }

    public function testExtractInsertSelectSqlLowercase(): void
    {
        $parser = new PgSqlParser();
        $sql = $parser->extractInsertSelectSql('insert into t (id) select id from old');
        self::assertNotNull($sql);
        self::assertStringStartsWith('select', $sql);
    }

    public function testExtractUpdateTableLowercase(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('users', $parser->extractUpdateTable("update users set name = 'x'"));
    }

    public function testExtractUpdateAliasLowercase(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('u', $parser->extractUpdateAlias("update users as u set name = 'x'"));
    }

    public function testExtractUpdateSetsLowercase(): void
    {
        $parser = new PgSqlParser();
        $sets = $parser->extractUpdateSets("update t set v = 1 where id = 2");
        self::assertSame(['v' => '1'], $sets);
    }

    public function testExtractWhereClauseLowercase(): void
    {
        $parser = new PgSqlParser();
        $where = $parser->extractWhereClause("delete from users where id = 1 returning *");
        self::assertSame('id = 1', $where);
    }

    public function testExtractUpdateFromClauseLowercase(): void
    {
        $parser = new PgSqlParser();
        $from = $parser->extractUpdateFromClause("update users set name = o.name from other o where users.id = o.id");
        self::assertSame('other o', $from);
    }

    public function testExtractDeleteTableLowercase(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('users', $parser->extractDeleteTable('delete from users where id = 1'));
    }

    public function testExtractDeleteAliasLowercase(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('u', $parser->extractDeleteAlias('delete from users as u where u.id = 1'));
    }

    public function testExtractDeleteUsingClauseLowercase(): void
    {
        $parser = new PgSqlParser();
        $using = $parser->extractDeleteUsingClause('delete from t using other where t.id = other.id');
        self::assertSame('other', $using);
    }

    public function testExtractTruncateTableLowercase(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('users', $parser->extractTruncateTable('truncate table users'));
    }

    public function testExtractCreateTableNameLowercase(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('users', $parser->extractCreateTableName('create table users (id int)'));
    }

    public function testHasIfNotExistsLowercase(): void
    {
        $parser = new PgSqlParser();
        self::assertTrue($parser->hasIfNotExists('create table if not exists t (id int)'));
    }

    public function testHasCreateTableAsSelectLowercase(): void
    {
        $parser = new PgSqlParser();
        self::assertTrue($parser->hasCreateTableAsSelect('create table t as select * from old'));
    }

    public function testExtractCreateTableSelectSqlLowercase(): void
    {
        $parser = new PgSqlParser();
        $sql = $parser->extractCreateTableSelectSql('create table t as select id from old');
        self::assertNotNull($sql);
        self::assertStringStartsWith('select', $sql);
    }

    public function testHasCreateTableLikeLowercase(): void
    {
        $parser = new PgSqlParser();
        self::assertTrue($parser->hasCreateTableLike('create table t (like old_t)'));
    }

    public function testExtractCreateTableLikeSourceLowercase(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('old_t', $parser->extractCreateTableLikeSource('create table t (like old_t)'));
    }

    public function testExtractDropTableNameLowercase(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('users', $parser->extractDropTableName('drop table users'));
    }

    public function testHasDropTableIfExistsLowercase(): void
    {
        $parser = new PgSqlParser();
        self::assertTrue($parser->hasDropTableIfExists('drop table if exists users'));
    }

    public function testExtractAlterTableNameLowercase(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('users', $parser->extractAlterTableName('alter table users add column x int'));
    }

    public function testExtractSelectTableNamesLowercase(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames('select * from users join orders on users.id = orders.uid');
        self::assertContains('users', $tables);
        self::assertContains('orders', $tables);
    }

    public function testExtractInsertTableOnlyLowercase(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('t', $parser->extractInsertTable('insert into only t (id) values (1)'));
    }

    public function testExtractUpdateTableOnlyLowercase(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('t', $parser->extractUpdateTable('update only t set x = 1'));
    }

    public function testExtractDeleteTableOnlyLowercase(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('t', $parser->extractDeleteTable('delete from only t where id = 1'));
    }

    public function testExtractTruncateTableOnlyLowercase(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('t', $parser->extractTruncateTable('truncate only t'));
    }

    public function testSplitStatementsExactOutput(): void
    {
        $parser = new PgSqlParser();
        $result = $parser->splitStatements('SELECT 1; SELECT 2; SELECT 3');
        self::assertSame(['SELECT 1', 'SELECT 2', 'SELECT 3'], $result);
    }

    public function testSplitStatementsPreservesContent(): void
    {
        $parser = new PgSqlParser();
        $result = $parser->splitStatements("SELECT 'a;b'; SELECT 2");
        self::assertSame(["SELECT 'a;b'", 'SELECT 2'], $result);
    }

    public function testExtractInsertValuesExactMultiple(): void
    {
        $parser = new PgSqlParser();
        $rows = $parser->extractInsertValues("INSERT INTO t (a, b) VALUES (1, 'x'), (2, 'y')");
        self::assertSame([['1', "'x'"], ['2', "'y'"]], $rows);
    }

    public function testExtractOnConflictWithTrailingSemicolon(): void
    {
        $parser = new PgSqlParser();
        $result = $parser->extractOnConflictUpdateColumns(
            "INSERT INTO t (id, v) VALUES (1, 2) ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v;"
        );
        self::assertSame(['v'], $result['columns']);
        self::assertSame('EXCLUDED.v', $result['values']['v']);
    }

    public function testExtractUpdateSetsMultipleColumns(): void
    {
        $parser = new PgSqlParser();
        $sets = $parser->extractUpdateSets("UPDATE t SET a = 1, b = 'x', c = NULL WHERE id = 1");
        self::assertSame('1', $sets['a']);
        self::assertSame("'x'", $sets['b']);
        self::assertSame('NULL', $sets['c']);
    }

    public function testExtractWhereClauseExactFromUpdate(): void
    {
        $parser = new PgSqlParser();
        $where = $parser->extractWhereClause("UPDATE t SET x = 1 WHERE id = 42 AND name = 'Bob'");
        self::assertSame("id = 42 AND name = 'Bob'", $where);
    }

    public function testExtractSelectTableNamesWithGroupBy(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames('SELECT count(*) FROM users GROUP BY name');
        self::assertContains('users', $tables);
    }

    public function testExtractSelectTableNamesWithLimit(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames('SELECT * FROM users LIMIT 10');
        self::assertContains('users', $tables);
    }

    public function testExtractSelectTableNamesWithOffset(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames('SELECT * FROM users OFFSET 5');
        self::assertContains('users', $tables);
    }

    public function testExtractSelectTableNamesWithHaving(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames('SELECT count(*) FROM users GROUP BY name HAVING count(*) > 1');
        self::assertContains('users', $tables);
    }

    public function testExtractSelectTableNamesUnion(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames('SELECT * FROM users UNION SELECT * FROM admins');
        self::assertContains('users', $tables);
        self::assertContains('admins', $tables);
    }

    public function testExtractSelectTableNamesForUpdate(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames('SELECT * FROM users FOR UPDATE');
        self::assertContains('users', $tables);
    }

    public function testStripLeadingBlockCommentOnly(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('SELECT', $parser->classifyStatement("/* block */ SELECT 1"));
    }

    public function testStripLeadingMixedComments(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('SELECT', $parser->classifyStatement("-- line\n/* block */\nSELECT 1"));
    }

    public function testClassifyWithMixedCase(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('SELECT', $parser->classifyStatement('Select * From users'));
        self::assertSame('INSERT', $parser->classifyStatement('Insert Into t (id) Values (1)'));
        self::assertSame('UPDATE', $parser->classifyStatement("Update t Set x = 1"));
        self::assertSame('DELETE', $parser->classifyStatement('Delete From t Where id = 1'));
    }

    public function testClassifyWithSemicolon(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('SELECT', $parser->classifyStatement('SELECT 1;'));
    }

    public function testExtractInsertColumnsWithSelectKeyword(): void
    {
        $parser = new PgSqlParser();
        $cols = $parser->extractInsertColumns("INSERT INTO t (id, name) SELECT id, name FROM old");
        self::assertSame(['id', 'name'], $cols);
    }

    public function testExtractSelectTableNamesWithSubqueryInWhere(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames('SELECT * FROM users WHERE id IN (SELECT uid FROM orders)');
        self::assertContains('users', $tables);
    }

    public function testSplitLineCommentAtEnd(): void
    {
        $parser = new PgSqlParser();
        $result = $parser->splitStatements('SELECT 1 -- comment');
        self::assertSame(['SELECT 1 -- comment'], $result);
    }

    public function testExtractOnConflictSetWithQuotedColumnsLowercase(): void
    {
        $parser = new PgSqlParser();
        $result = $parser->extractOnConflictUpdateColumns(
            'insert into t (id, "Col") values (1, 2) on conflict (id) do update set "Col" = excluded."Col"'
        );
        self::assertSame(['Col'], $result['columns']);
    }

    public function testExtractWhereClauseFromUpdateWithFrom(): void
    {
        $parser = new PgSqlParser();
        $where = $parser->extractWhereClause("UPDATE t SET v = o.v FROM other o WHERE t.id = o.id");
        self::assertNotNull($where);
        self::assertStringContainsString('t.id = o.id', $where);
    }

    public function testExtractDeleteAliasEndOfStatement(): void
    {
        $parser = new PgSqlParser();
        self::assertNull($parser->extractDeleteAlias('DELETE FROM users'));
    }

    public function testHasInsertSelectWithValuesFalse(): void
    {
        $parser = new PgSqlParser();
        self::assertFalse($parser->hasInsertSelect("INSERT INTO t (id) VALUES (1)"));
    }

    public function testExtractUpdateSetsLowercaseReturning(): void
    {
        $parser = new PgSqlParser();
        $sets = $parser->extractUpdateSets("update t set v = 1 returning *");
        self::assertSame(['v' => '1'], $sets);
    }

    public function testExtractInsertTableWithAsAlias(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('t', $parser->extractInsertTable('insert into t as alias (id) values (1)'));
    }

    public function testExtractCreateTableNameUnlogged(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('t', $parser->extractCreateTableName('CREATE UNLOGGED TABLE t (id INT)'));
    }

    public function testExtractAlterTableNameSchemaQualified(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('users', $parser->extractAlterTableName('ALTER TABLE public.users ADD COLUMN x INT'));
    }

    public function testExtractDropTableNameQuoted(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('My Table', $parser->extractDropTableName('DROP TABLE "My Table"'));
    }

    public function testSplitDollarQuoteAlphanumTag(): void
    {
        $parser = new PgSqlParser();
        $result = $parser->splitStatements('SELECT $fn$body;here$fn$; SELECT 2');
        self::assertSame(2, count($result));
        self::assertStringContainsString('body;here', $result[0]);
    }

    public function testExtractSelectTableNamesIntersect(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames('SELECT id FROM users INTERSECT SELECT id FROM admins');
        self::assertContains('users', $tables);
    }

    public function testExtractSelectTableNamesExcept(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames('SELECT id FROM users EXCEPT SELECT id FROM banned');
        self::assertContains('users', $tables);
    }

    public function testExtractUpdateSetsWithSingleQuoteInValue(): void
    {
        $parser = new PgSqlParser();
        $sets = $parser->extractUpdateSets("UPDATE t SET name = 'it''s' WHERE id = 1");
        self::assertSame("'it''s'", $sets['name']);
    }

    public function testExtractSelectTableNamesWithAs(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames('SELECT * FROM users AS u');
        self::assertContains('users', $tables);
    }

    public function testExtractInsertValuesMultiLine(): void
    {
        $parser = new PgSqlParser();
        $values = $parser->extractInsertValues("INSERT INTO t (a)\nVALUES\n(1)");
        self::assertSame([['1']], $values);
    }

    public function testExtractOnConflictUpdateColumnsLowercase(): void
    {
        $parser = new PgSqlParser();
        $info = $parser->extractOnConflictUpdateColumns(
            "insert into t (id, name) values (1, 'a') on conflict (id) do update set name = excluded.name"
        );
        self::assertSame(['name'], $info['columns']);
        self::assertSame(['name' => 'excluded.name'], $info['values']);
    }

    public function testExtractOnConflictUpdateColumnsMultiLine(): void
    {
        $parser = new PgSqlParser();
        $info = $parser->extractOnConflictUpdateColumns(
            "INSERT INTO t (id, name) VALUES (1, 'a')\nON CONFLICT (id)\nDO UPDATE SET\nname = EXCLUDED.name"
        );
        self::assertSame(['name'], $info['columns']);
    }

    public function testHasInsertSelectMultiLine(): void
    {
        $parser = new PgSqlParser();
        self::assertTrue($parser->hasInsertSelect("INSERT INTO t\nSELECT * FROM s"));
    }

    public function testExtractUpdateSetsMultiLine(): void
    {
        $parser = new PgSqlParser();
        $sets = $parser->extractUpdateSets("UPDATE t\nSET\nname = 'Bob'\nWHERE id = 1");
        self::assertSame("'Bob'", $sets['name']);
    }

    public function testExtractWhereClauseMultiLine(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('id = 1', $parser->extractWhereClause("UPDATE t SET name = 'Bob'\nWHERE id = 1"));
    }

    public function testExtractUpdateFromClauseMultiLine(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('orders', $parser->extractUpdateFromClause("UPDATE users SET name = orders.name\nFROM orders\nWHERE users.id = orders.user_id"));
    }

    public function testExtractDeleteUsingClauseMultiLine(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('orders', $parser->extractDeleteUsingClause("DELETE FROM users\nUSING orders\nWHERE users.id = orders.user_id"));
    }

    public function testHasCreateTableAsSelectMultiLine(): void
    {
        $parser = new PgSqlParser();
        self::assertTrue($parser->hasCreateTableAsSelect("CREATE TABLE t\nAS SELECT 1"));
    }

    public function testExtractCreateTableSelectSqlMultiLine(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('SELECT 1', $parser->extractCreateTableSelectSql("CREATE TABLE t\nAS SELECT 1"));
    }

    public function testHasCreateTableLikeMultiLine(): void
    {
        $parser = new PgSqlParser();
        self::assertTrue($parser->hasCreateTableLike("CREATE TABLE t (\nLIKE s)"));
    }

    public function testExtractSelectTableNamesWithJoinLowercase(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames('select * from users inner join orders on users.id = orders.user_id');
        self::assertContains('users', $tables);
    }

    public function testExtractSelectTableNamesMultiLine(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames("SELECT *\nFROM users\nINNER JOIN orders\nON users.id = orders.user_id");
        self::assertContains('users', $tables);
    }

    public function testExtractUpdateSetsWithNewlineInValue(): void
    {
        $parser = new PgSqlParser();
        $sets = $parser->extractUpdateSets("UPDATE t SET bio = 'line1\nline2' WHERE id = 1");
        self::assertArrayHasKey('bio', $sets);
    }

    public function testExtractOnConflictAssignmentMultiLine(): void
    {
        $parser = new PgSqlParser();
        $info = $parser->extractOnConflictUpdateColumns(
            "INSERT INTO t (id, a, b) VALUES (1, 2, 3)\nON CONFLICT (id) DO UPDATE SET\na = EXCLUDED.a,\nb = EXCLUDED.b"
        );
        self::assertSame(['a', 'b'], $info['columns']);
    }

    public function testExtractInsertValuesWithLeadingWhitespace(): void
    {
        $parser = new PgSqlParser();
        $values = $parser->extractInsertValues("INSERT INTO t (a) VALUES ( 1 )");
        self::assertSame([['1']], $values);
    }

    public function testExtractInsertValuesMultipleRows(): void
    {
        $parser = new PgSqlParser();
        $values = $parser->extractInsertValues("INSERT INTO t (a) VALUES (1), (2)");
        self::assertSame([['1'], ['2']], $values);
    }

    public function testExtractUpdateSetsOnlySetColumns(): void
    {
        $parser = new PgSqlParser();
        $sets = $parser->extractUpdateSets("UPDATE t SET a = 1, b = 2 FROM s WHERE t.id = s.id");
        self::assertSame(['a' => '1', 'b' => '2'], $sets);
    }

    public function testExtractWhereClauseTrimmed(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('id = 1', $parser->extractWhereClause("DELETE FROM t WHERE   id = 1  "));
    }

    public function testExtractUpdateFromClauseTrimmed(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('orders', $parser->extractUpdateFromClause("UPDATE users SET name = orders.name FROM  orders  WHERE users.id = orders.user_id"));
    }

    public function testExtractDeleteUsingClauseTrimmed(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('orders', $parser->extractDeleteUsingClause("DELETE FROM users USING  orders  WHERE users.id = orders.user_id"));
    }

    public function testHasCreateTableAsSelectTemporary(): void
    {
        $parser = new PgSqlParser();
        self::assertTrue($parser->hasCreateTableAsSelect('CREATE TEMPORARY TABLE t AS SELECT 1'));
    }

    public function testHasCreateTableAsSelectTemp(): void
    {
        $parser = new PgSqlParser();
        self::assertTrue($parser->hasCreateTableAsSelect('CREATE TEMP TABLE t AS SELECT 1'));
    }

    public function testHasCreateTableAsSelectUnlogged(): void
    {
        $parser = new PgSqlParser();
        self::assertTrue($parser->hasCreateTableAsSelect('CREATE UNLOGGED TABLE t AS SELECT 1'));
    }

    public function testHasCreateTableLikeTemporary(): void
    {
        $parser = new PgSqlParser();
        self::assertTrue($parser->hasCreateTableLike('CREATE TEMPORARY TABLE t (LIKE s)'));
    }

    public function testExtractOnConflictUpdateColumnsWithWhere(): void
    {
        $parser = new PgSqlParser();
        $info = $parser->extractOnConflictUpdateColumns(
            "INSERT INTO t (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name WHERE t.id > 0"
        );
        self::assertSame(['name'], $info['columns']);
    }

    public function testExtractInsertValuesNewlineInsideParens(): void
    {
        $parser = new PgSqlParser();
        $values = $parser->extractInsertValues("INSERT INTO t (a, b) VALUES (\n1,\n2\n)");
        self::assertSame([['1', '2']], $values);
    }

    public function testExtractOnConflictNewlineInsideSetClause(): void
    {
        $parser = new PgSqlParser();
        $info = $parser->extractOnConflictUpdateColumns(
            "INSERT INTO t (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name =\nEXCLUDED.name"
        );
        self::assertSame(['name'], $info['columns']);
        self::assertSame("EXCLUDED.name", $info['values']['name']);
    }

    public function testExtractUpdateSetsNewlineInAssignment(): void
    {
        $parser = new PgSqlParser();
        $sets = $parser->extractUpdateSets("UPDATE t SET name =\n'Bob' WHERE id = 1");
        self::assertSame("'Bob'", $sets['name']);
    }

    public function testExtractUpdateSetsMultiLineFromClause(): void
    {
        $parser = new PgSqlParser();
        $sets = $parser->extractUpdateSets("UPDATE t SET a = 1\nFROM other\nWHERE t.id = other.id");
        self::assertSame(['a' => '1'], $sets);
    }

    public function testExtractWhereClauseNewlineBeforeReturning(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('id = 1', $parser->extractWhereClause("UPDATE t SET name = 'Bob' WHERE id = 1\nRETURNING *"));
    }

    public function testExtractWhereClauseNewlineBeforeOrderBy(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('id > 0', $parser->extractWhereClause("DELETE FROM t WHERE id > 0\nORDER BY id"));
    }

    public function testExtractWhereClauseNewlineBeforeLimit(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('id > 0', $parser->extractWhereClause("DELETE FROM t WHERE id > 0\nLIMIT 10"));
    }

    public function testExtractUpdateFromClauseNewlineAfterFrom(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('orders', $parser->extractUpdateFromClause("UPDATE users SET name = orders.name\nFROM\norders\nWHERE users.id = orders.user_id"));
    }

    public function testExtractDeleteUsingClauseNewlineAfterUsing(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('orders', $parser->extractDeleteUsingClause("DELETE FROM users\nUSING\norders\nWHERE users.id = orders.user_id"));
    }

    public function testExtractCreateTableSelectSqlNewlineBeforeSelect(): void
    {
        $parser = new PgSqlParser();
        self::assertSame("SELECT\n1", $parser->extractCreateTableSelectSql("CREATE TABLE t AS\nSELECT\n1"));
    }

    public function testHasCreateTableLikeNewlineBeforeLike(): void
    {
        $parser = new PgSqlParser();
        self::assertTrue($parser->hasCreateTableLike("CREATE TABLE t (\nLIKE s\n)"));
    }

    public function testHasInsertSelectNewlineBeforeSelect(): void
    {
        $parser = new PgSqlParser();
        self::assertTrue($parser->hasInsertSelect("INSERT INTO t\nSELECT * FROM s"));
    }

    public function testExtractSelectTableNamesNewlineSeparatedFromClause(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames("SELECT *\nFROM\nusers\nWHERE id = 1");
        self::assertContains('users', $tables);
    }

    public function testHasCreateTableAsSelectNewlineBeforeAs(): void
    {
        $parser = new PgSqlParser();
        self::assertTrue($parser->hasCreateTableAsSelect("CREATE TABLE t\nAS\nSELECT 1"));
    }

    public function testExtractUpdateFromClauseNewlineBeforeReturning(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('orders', $parser->extractUpdateFromClause("UPDATE users SET name = orders.name FROM orders\nRETURNING *\nWHERE users.id = orders.user_id"));
    }

    public function testExtractDeleteUsingClauseNewlineBeforeReturning(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('orders', $parser->extractDeleteUsingClause("DELETE FROM users USING orders\nRETURNING *\nWHERE users.id = orders.user_id"));
    }

    public function testSplitStatementsNewlineInDollarQuote(): void
    {
        $parser = new PgSqlParser();
        $stmts = $parser->splitStatements('SELECT $$hello' . "\n" . 'world$$');
        self::assertCount(1, $stmts);
        self::assertSame('SELECT $$hello' . "\n" . 'world$$', $stmts[0]);
    }

    public function testSplitStatementsNewlineInSingleQuote(): void
    {
        $parser = new PgSqlParser();
        $stmts = $parser->splitStatements("SELECT 'hello\nworld'");
        self::assertCount(1, $stmts);
        self::assertSame("SELECT 'hello\nworld'", $stmts[0]);
    }

    public function testExtractInsertValuesWithEscapedSingleQuoteInValue(): void
    {
        $parser = new PgSqlParser();
        $values = $parser->extractInsertValues("INSERT INTO t (name) VALUES ('it''s ok')");
        self::assertSame([["'it''s ok'"]], $values);
    }

    public function testExtractOnConflictSetMultipleColumnsExact(): void
    {
        $parser = new PgSqlParser();
        $info = $parser->extractOnConflictUpdateColumns(
            "INSERT INTO t (id, a, b) VALUES (1, 2, 3) ON CONFLICT (id) DO UPDATE SET a = EXCLUDED.a, b = EXCLUDED.b"
        );
        self::assertSame(['a', 'b'], $info['columns']);
        self::assertSame(['a' => 'EXCLUDED.a', 'b' => 'EXCLUDED.b'], $info['values']);
    }

    public function testExtractSelectTableNamesWithInlineJoin(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames("SELECT * FROM users\nINNER JOIN orders ON users.id = orders.uid");
        self::assertContains('users', $tables);
        self::assertContains('orders', $tables);
    }

    public function testExtractSelectTableNamesWithCrossJoin(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames("SELECT * FROM users CROSS JOIN orders");
        self::assertContains('users', $tables);
    }

    public function testExtractSelectTableNamesWithNaturalJoin(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames("SELECT * FROM users NATURAL JOIN orders");
        self::assertContains('users', $tables);
    }

    public function testExtractOnConflictWithTrailingWhitespace(): void
    {
        $parser = new PgSqlParser();
        $info = $parser->extractOnConflictUpdateColumns(
            "INSERT INTO t (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name   "
        );
        self::assertSame('EXCLUDED.name', $info['values']['name']);
    }

    public function testClassifyWithLeadingBlockCommentAndLineComment(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('SELECT', $parser->classifyStatement("/* comment */\n-- line\nSELECT 1"));
    }

    public function testExtractInsertTableWithSchemaLowercase(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('users', $parser->extractInsertTable('insert into public.users (id) values (1)'));
    }

    public function testExtractUpdateTableLowercaseOnly(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('users', $parser->extractUpdateTable("update only users set name = 'x'"));
    }

    public function testExtractDeleteTableLowercaseOnly(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('users', $parser->extractDeleteTable('delete from only users where id = 1'));
    }

    public function testExtractSelectTableNamesWithFullOuterJoin(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames('SELECT * FROM users FULL OUTER JOIN orders ON users.id = orders.uid');
        self::assertContains('users', $tables);
        self::assertContains('orders', $tables);
    }

    public function testExtractSelectTableNamesWithRightJoin(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames('SELECT * FROM users RIGHT JOIN orders ON users.id = orders.uid');
        self::assertContains('users', $tables);
        self::assertContains('orders', $tables);
    }

    public function testExtractOnConflictColumnTrimmed(): void
    {
        $parser = new PgSqlParser();
        $info = $parser->extractOnConflictUpdateColumns(
            "INSERT INTO t (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET  name  =  EXCLUDED.name  "
        );
        self::assertSame(['name'], $info['columns']);
        self::assertSame('EXCLUDED.name', $info['values']['name']);
    }

    public function testClassifyWithDeleteTerminal(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('DELETE', $parser->classifyStatement('WITH old AS (SELECT 1) DELETE FROM users WHERE id IN (SELECT * FROM old)'));
    }

    public function testClassifyWithUpdateTerminal(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('UPDATE', $parser->classifyStatement("WITH vals AS (SELECT 1) UPDATE users SET name = 'x' WHERE id IN (SELECT * FROM vals)"));
    }

    public function testExtractWhereClauseFromDeleteWithUsing(): void
    {
        $parser = new PgSqlParser();
        self::assertNotNull($parser->extractWhereClause('DELETE FROM users USING orders WHERE users.id = orders.user_id'));
    }

    public function testExtractUpdateSetsReturningSensitive(): void
    {
        $parser = new PgSqlParser();
        $sets = $parser->extractUpdateSets("UPDATE t SET a = 1 RETURNING *");
        self::assertSame(['a' => '1'], $sets);
    }

    public function testExtractSelectTableNamesCommaJoined(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames('SELECT * FROM users, orders WHERE users.id = orders.uid');
        self::assertContains('users', $tables);
        self::assertContains('orders', $tables);
    }

    public function testExtractSelectTableNamesQuotedTable(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames('SELECT * FROM "Users" WHERE id = 1');
        self::assertContains('Users', $tables);
    }

    public function testHasOnConflictDoNothingLowercase(): void
    {
        $parser = new PgSqlParser();
        self::assertTrue($parser->hasOnConflict('insert into t (id) values (1) on conflict do nothing'));
    }

    public function testExtractOnConflictDoNothingReturnsEmptyColumns(): void
    {
        $parser = new PgSqlParser();
        $info = $parser->extractOnConflictUpdateColumns('INSERT INTO t (id) VALUES (1) ON CONFLICT (id) DO NOTHING');
        self::assertSame([], $info['columns']);
    }

    public function testStripSchemaPrefixUnquotedSchema(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('users', $parser->stripSchemaPrefix('public.users'));
    }

    public function testStripSchemaPrefixQuotedSchemaQuotedTable(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('"users"', $parser->stripSchemaPrefix('"public"."users"'));
    }

    public function testStripSchemaPrefixNoSchema(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('users', $parser->stripSchemaPrefix('users'));
    }

    public function testUnquoteIdentifierNoQuotes(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('users', $parser->unquoteIdentifier('users'));
    }

    public function testUnquoteIdentifierWithDoubleQuotes(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('Users', $parser->unquoteIdentifier('"Users"'));
    }

    public function testUnquoteIdentifierEscapedDoubleQuotes(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('col"name', $parser->unquoteIdentifier('"col""name"'));
    }

    public function testExtractInsertColumnsFromMultilineInsert(): void
    {
        $parser = new PgSqlParser();
        $cols = $parser->extractInsertColumns("INSERT INTO t\n(a, b)\nVALUES (1, 2)");
        self::assertSame(['a', 'b'], $cols);
    }

    public function testExtractWhereClauseWithOrderByMultiWord(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('id > 0', $parser->extractWhereClause("DELETE FROM t WHERE id > 0 ORDER  BY id"));
    }

    public function testExtractSelectTableNamesSchemaQualifiedInFrom(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames('SELECT * FROM public.users');
        self::assertContains('users', $tables);
    }

    public function testExtractInsertValuesWithNewlineAfterValues(): void
    {
        $parser = new PgSqlParser();
        $values = $parser->extractInsertValues("INSERT INTO t (a) VALUES\n(1)");
        self::assertSame([['1']], $values);
    }

    public function testHasInsertSelectWithColumnsBeforeSelect(): void
    {
        $parser = new PgSqlParser();
        self::assertTrue($parser->hasInsertSelect('INSERT INTO t (a, b) SELECT a, b FROM s'));
    }

    public function testExtractInsertSelectSqlExact(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('SELECT a, b FROM s', $parser->extractInsertSelectSql('INSERT INTO t (a, b) SELECT a, b FROM s'));
    }

    public function testClassifyWithRecursive(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('SELECT', $parser->classifyStatement('WITH RECURSIVE cte AS (SELECT 1 UNION ALL SELECT n+1 FROM cte WHERE n < 10) SELECT * FROM cte'));
    }

    public function testSplitStatementsPreservesEscapedBackslash(): void
    {
        $parser = new PgSqlParser();
        $stmts = $parser->splitStatements("SELECT E'a\\\\b'");
        self::assertCount(1, $stmts);
        self::assertSame("SELECT E'a\\\\b'", $stmts[0]);
    }

    public function testExtractSelectTableNamesSubqueryNotIncluded(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames('SELECT * FROM (SELECT 1) AS sub');
        self::assertNotContains('sub', $tables);
    }

    public function testExtractUpdateSetsWithSubqueryInValue(): void
    {
        $parser = new PgSqlParser();
        $sets = $parser->extractUpdateSets("UPDATE t SET count = (SELECT COUNT(*) FROM other) WHERE id = 1");
        self::assertArrayHasKey('count', $sets);
    }

    public function testExtractOnConflictWithQuotedColumnInSet(): void
    {
        $parser = new PgSqlParser();
        $info = $parser->extractOnConflictUpdateColumns(
            'INSERT INTO t (id, "Name") VALUES (1, \'a\') ON CONFLICT (id) DO UPDATE SET "Name" = EXCLUDED."Name"'
        );
        self::assertSame(['Name'], $info['columns']);
    }

    public function testExtractDeleteAliasWithUsing(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('u', $parser->extractDeleteAlias('DELETE FROM users u USING orders WHERE u.id = orders.user_id'));
    }

    public function testExtractDeleteAliasWithWhere(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('u', $parser->extractDeleteAlias('DELETE FROM users u WHERE u.id = 1'));
    }

    public function testExtractDeleteAliasWithReturning(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('u', $parser->extractDeleteAlias('DELETE FROM users u RETURNING *'));
    }

    public function testClassifySetTransactionAsTcl(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('TCL', $parser->classifyStatement('SET TRANSACTION ISOLATION LEVEL READ COMMITTED'));
    }

    public function testClassifySavepointAsTcl(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('TCL', $parser->classifyStatement('SAVEPOINT my_save'));
    }

    public function testClassifyReleaseSavepointAsTcl(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('TCL', $parser->classifyStatement('RELEASE SAVEPOINT my_save'));
    }

    public function testClassifyStartTransactionAsTcl(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('TCL', $parser->classifyStatement('START TRANSACTION'));
    }

    public function testExtractInsertValuesMultipleRowsWithSpaces(): void
    {
        $parser = new PgSqlParser();
        $values = $parser->extractInsertValues("INSERT INTO t (a) VALUES  ( 1 ) , ( 2 )");
        self::assertSame([['1'], ['2']], $values);
    }

    public function testExtractUpdateAliasWithoutAs(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('u', $parser->extractUpdateAlias("UPDATE users u SET name = 'x'"));
    }

    public function testExtractUpdateAliasReturnsNullNoAlias(): void
    {
        $parser = new PgSqlParser();
        self::assertNull($parser->extractUpdateAlias("UPDATE users SET name = 'x'"));
    }

    public function testExtractInsertSelectSqlReturnsNullNoSelect(): void
    {
        $parser = new PgSqlParser();
        self::assertNull($parser->extractInsertSelectSql("INSERT INTO t (a) VALUES (1)"));
    }

    public function testExtractCreateTableLikeSourceReturnsNullNoLike(): void
    {
        $parser = new PgSqlParser();
        self::assertNull($parser->extractCreateTableLikeSource('CREATE TABLE t (id INTEGER)'));
    }

    public function testClassifyWithLeadingLineCommentNoNewline(): void
    {
        $parser = new PgSqlParser();
        self::assertNull($parser->classifyStatement('-- comment only'));
    }

    public function testExtractSelectTableNamesMultipleFromClauses(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames('SELECT * FROM users WHERE id IN (SELECT uid FROM orders)');
        self::assertContains('users', $tables);
        self::assertContains('orders', $tables);
    }

    public function testExtractWhereClauseFromMultilineUpdate(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('id = 1', $parser->extractWhereClause("UPDATE t\nSET name = 'Bob'\nWHERE\nid = 1"));
    }

    public function testHasCreateTableAsSelectIfNotExists(): void
    {
        $parser = new PgSqlParser();
        self::assertTrue($parser->hasCreateTableAsSelect('CREATE TABLE IF NOT EXISTS t AS SELECT 1'));
    }

    public function testExtractDeleteAliasAtEndOfStatement(): void
    {
        $parser = new PgSqlParser();
        self::assertNull($parser->extractDeleteAlias('DELETE FROM users AS u'));
    }

    public function testExtractOnConflictWhereClauseRemoved(): void
    {
        $parser = new PgSqlParser();
        $info = $parser->extractOnConflictUpdateColumns(
            "INSERT INTO t (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name WHERE t.active = true"
        );
        self::assertSame(['name'], $info['columns']);
        self::assertSame('EXCLUDED.name', $info['values']['name']);
    }

    public function testExtractInsertValuesEscapedQuoteMiddle(): void
    {
        $parser = new PgSqlParser();
        $values = $parser->extractInsertValues("INSERT INTO t (a, b) VALUES ('it''s', 'ok')");
        self::assertCount(1, $values);
        self::assertCount(2, $values[0]);
    }

    public function testExtractInsertTableReturnsNullOnEmpty(): void
    {
        $parser = new PgSqlParser();
        self::assertNull($parser->extractInsertTable('NOT AN INSERT'));
    }

    public function testExtractDeleteTableReturnsNullOnNonDelete(): void
    {
        $parser = new PgSqlParser();
        self::assertNull($parser->extractDeleteTable('SELECT * FROM users'));
    }

    public function testExtractCreateTableNameReturnsNullNonCreate(): void
    {
        $parser = new PgSqlParser();
        self::assertNull($parser->extractCreateTableName('SELECT 1'));
    }

    public function testExtractDropTableNameReturnsNullNonDrop(): void
    {
        $parser = new PgSqlParser();
        self::assertNull($parser->extractDropTableName('SELECT 1'));
    }

    public function testExtractTruncateTableReturnsNullNonTruncate(): void
    {
        $parser = new PgSqlParser();
        self::assertNull($parser->extractTruncateTable('SELECT 1'));
    }

    public function testExtractAlterTableNameReturnsNullNonAlter(): void
    {
        $parser = new PgSqlParser();
        self::assertNull($parser->extractAlterTableName('SELECT 1'));
    }

    public function testSplitByTopLevelCommaViaUpdateSetsWithParentheses(): void
    {
        $parser = new PgSqlParser();
        $sets = $parser->extractUpdateSets("UPDATE t SET a = fn(1, 2), b = 3 WHERE id = 1");
        self::assertSame(['a' => 'fn(1, 2)', 'b' => '3'], $sets);
    }

    public function testSplitByTopLevelCommaViaSetsWithSingleQuotedComma(): void
    {
        $parser = new PgSqlParser();
        $sets = $parser->extractUpdateSets("UPDATE t SET name = 'a,b', val = 1 WHERE id = 1");
        self::assertSame(['name' => "'a,b'", 'val' => '1'], $sets);
    }

    public function testSplitByTopLevelCommaViaSetsWithDoubleQuotedComma(): void
    {
        $parser = new PgSqlParser();
        $sets = $parser->extractUpdateSets('UPDATE t SET "a,b" = 1, c = 2 WHERE id = 1');
        self::assertSame(['a,b' => '1', 'c' => '2'], $sets);
    }

    public function testSplitByTopLevelCommaViaSetsWithEscapedQuote(): void
    {
        $parser = new PgSqlParser();
        $sets = $parser->extractUpdateSets("UPDATE t SET name = 'it''s', val = 1 WHERE id = 1");
        self::assertSame(['name' => "'it''s'", 'val' => '1'], $sets);
    }

    public function testSplitByTopLevelCommaViaSetsWithQuotedColumn(): void
    {
        $parser = new PgSqlParser();
        $sets = $parser->extractUpdateSets('UPDATE t SET "col_a" = 1, b = 2 WHERE id = 1');
        self::assertSame(['col_a' => '1', 'b' => '2'], $sets);
    }

    public function testExtractSelectTableNamesSkipsSubqueryInFrom(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames('SELECT * FROM users, (SELECT 1) sub');
        self::assertContains('users', $tables);
        self::assertNotContains('1', $tables);
    }

    public function testExtractSelectTableNamesWithLateralJoin(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames('SELECT * FROM users JOIN orders ON users.id = orders.uid WHERE 1=1');
        self::assertContains('users', $tables);
        self::assertContains('orders', $tables);
    }

    public function testClassifyWithStatementQuotedIdentifier(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('SELECT', $parser->classifyStatement('WITH "my cte" AS (SELECT 1) SELECT * FROM "my cte"'));
    }

    public function testHasInsertSelectFalseForInsertValues(): void
    {
        $parser = new PgSqlParser();
        self::assertFalse($parser->hasInsertSelect("INSERT INTO t (id) VALUES (1)"));
    }

    public function testHasOnConflictFalseForPlainInsert(): void
    {
        $parser = new PgSqlParser();
        self::assertFalse($parser->hasOnConflict('INSERT INTO t (id) VALUES (1)'));
    }

    public function testHasIfNotExistsFalse(): void
    {
        $parser = new PgSqlParser();
        self::assertFalse($parser->hasIfNotExists('CREATE TABLE t (id INTEGER)'));
    }

    public function testHasDropTableIfExistsFalse(): void
    {
        $parser = new PgSqlParser();
        self::assertFalse($parser->hasDropTableIfExists('DROP TABLE t'));
    }

    public function testHasCreateTableAsSelectFalse(): void
    {
        $parser = new PgSqlParser();
        self::assertFalse($parser->hasCreateTableAsSelect('CREATE TABLE t (id INTEGER)'));
    }

    public function testHasCreateTableLikeFalse(): void
    {
        $parser = new PgSqlParser();
        self::assertFalse($parser->hasCreateTableLike('CREATE TABLE t (id INTEGER)'));
    }

    public function testExtractUpdateSetsEmptySetClause(): void
    {
        $parser = new PgSqlParser();
        $sets = $parser->extractUpdateSets('UPDATE t WHERE id = 1');
        self::assertSame([], $sets);
    }

    public function testExtractInsertColumnsNoParens(): void
    {
        $parser = new PgSqlParser();
        $cols = $parser->extractInsertColumns('INSERT INTO t VALUES (1)');
        self::assertSame([], $cols);
    }

    public function testExtractInsertValuesNoValues(): void
    {
        $parser = new PgSqlParser();
        $values = $parser->extractInsertValues('INSERT INTO t SELECT * FROM s');
        self::assertSame([], $values);
    }

    public function testClassifyWithCteAlias(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('INSERT', $parser->classifyStatement('WITH data AS (SELECT 1 AS id) INSERT INTO t SELECT * FROM data'));
    }

    public function testExtractInsertValuesWithNewlineInStringValue(): void
    {
        $parser = new PgSqlParser();
        $values = $parser->extractInsertValues("INSERT INTO t (note) VALUES ('line1\nline2')");
        self::assertCount(1, $values);
    }

    public function testExtractSelectTableNamesForUpdateClause(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames('SELECT * FROM users FOR UPDATE');
        self::assertContains('users', $tables);
    }

    public function testExtractUpdateFromClauseWithMultipleTables(): void
    {
        $parser = new PgSqlParser();
        $from = $parser->extractUpdateFromClause("UPDATE t SET a = s.a FROM s, r WHERE t.id = s.id");
        self::assertNotNull($from);
        self::assertStringContainsString('s', $from);
    }

    public function testExtractWhereClauseExactTrimming(): void
    {
        $parser = new PgSqlParser();
        $where = $parser->extractWhereClause("DELETE FROM t WHERE    id = 1   ");
        self::assertSame('id = 1', $where);
    }

    public function testExtractUpdateFromClauseExactTrimming(): void
    {
        $parser = new PgSqlParser();
        $from = $parser->extractUpdateFromClause("UPDATE t SET a = s.a FROM   other_table   WHERE t.id = other_table.id");
        self::assertSame('other_table', $from);
    }

    public function testExtractDeleteUsingClauseExactTrimming(): void
    {
        $parser = new PgSqlParser();
        $using = $parser->extractDeleteUsingClause("DELETE FROM t USING   other_table   WHERE t.id = other_table.id");
        self::assertSame('other_table', $using);
    }

    public function testExtractCreateTableSelectSqlExactTrimming(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('SELECT 1', $parser->extractCreateTableSelectSql("CREATE TABLE t AS   SELECT 1  "));
    }

    public function testClassifyEmptyStringReturnsNull(): void
    {
        $parser = new PgSqlParser();
        self::assertNull($parser->classifyStatement(''));
    }

    public function testExtractSelectTableNamesWithGroupByClause(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames('SELECT count(*) FROM users GROUP BY name');
        self::assertContains('users', $tables);
    }

    public function testExtractSelectTableNamesWithHavingClause(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames('SELECT count(*) FROM users GROUP BY name HAVING count(*) > 1');
        self::assertContains('users', $tables);
    }

    public function testExtractSelectTableNamesWithOffsetClause(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames('SELECT * FROM users LIMIT 10 OFFSET 5');
        self::assertContains('users', $tables);
    }

    public function testExtractOnConflictUpdateColumnsMultilineAssignment(): void
    {
        $parser = new PgSqlParser();
        $sql = "INSERT INTO users (id, name) VALUES (1, 'Alice') ON CONFLICT (id) DO UPDATE SET name =\n'Bob'";
        $result = $parser->extractOnConflictUpdateColumns($sql);
        self::assertSame(['name'], $result['columns']);
    }

    public function testHasInsertSelectLowercaseValues(): void
    {
        $parser = new PgSqlParser();
        self::assertFalse($parser->hasInsertSelect("INSERT INTO users (id) values (1)"));
    }

    public function testExtractWhereClauseWithMultilineReturning(): void
    {
        $parser = new PgSqlParser();
        $sql = "UPDATE users SET name = 'Bob' WHERE id = 1\nRETURNING *";
        $where = $parser->extractWhereClause($sql);
        self::assertSame('id = 1', $where);
    }

    public function testExtractWhereClauseWithMultilineOrderBy(): void
    {
        $parser = new PgSqlParser();
        $sql = "DELETE FROM users WHERE active = false\nORDER BY id";
        $where = $parser->extractWhereClause($sql);
        self::assertSame('active = false', $where);
    }

    public function testExtractWhereClauseWithMultilineLimit(): void
    {
        $parser = new PgSqlParser();
        $sql = "DELETE FROM users WHERE active = false\nLIMIT 10";
        $where = $parser->extractWhereClause($sql);
        self::assertSame('active = false', $where);
    }

    public function testExtractUpdateFromClauseMultilineFrom(): void
    {
        $parser = new PgSqlParser();
        $sql = "UPDATE users SET name = orders.name\nFROM\norders WHERE users.id = orders.user_id";
        $from = $parser->extractUpdateFromClause($sql);
        self::assertNotNull($from);
        self::assertStringContainsString('orders', $from);
    }

    public function testExtractSelectTableNamesLowercaseTableRef(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames('select * from users join orders on users.id = orders.user_id');
        self::assertContains('users', $tables);
        self::assertContains('orders', $tables);
    }

    public function testExtractUpdateSetsMultilineAssignment(): void
    {
        $parser = new PgSqlParser();
        $sql = "UPDATE users SET bio =\n'hello\nworld' WHERE id = 1";
        $sets = $parser->extractUpdateSets($sql);
        self::assertArrayHasKey('bio', $sets);
    }

    public function testExtractInsertValuesMultilineInsideParens(): void
    {
        $parser = new PgSqlParser();
        $sql = "INSERT INTO users (id, name) VALUES (1,\n'Alice')";
        $vals = $parser->extractInsertValues($sql);
        self::assertCount(1, $vals);
        self::assertCount(2, $vals[0]);
    }

    public function testClassifyInsertSelectNotMisclassifiedAsSelect(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('INSERT', $parser->classifyStatement('INSERT INTO users SELECT 1'));
    }

    public function testClassifyUpdateContainingSelectNotMisclassified(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('UPDATE', $parser->classifyStatement("UPDATE users SET note = 'SELECT me' WHERE id = 1"));
    }

    public function testClassifyDeleteContainingInsertNotMisclassified(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('DELETE', $parser->classifyStatement("DELETE FROM users WHERE note = 'INSERT this'"));
    }

    public function testClassifyTruncateWithDeleteInComment(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('TRUNCATE', $parser->classifyStatement('TRUNCATE TABLE t /* DELETE FROM t */'));
    }

    public function testClassifyCreateTableWithTruncateInDefault(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('CREATE_TABLE', $parser->classifyStatement("CREATE TABLE t (note TEXT DEFAULT 'TRUNCATE TABLE x')"));
    }

    public function testClassifyDropTableWithCreateTableInComment(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('DROP_TABLE', $parser->classifyStatement('DROP TABLE t /* CREATE TABLE backup (id INT) */'));
    }

    public function testClassifyAlterTableWithDropTableInComment(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('ALTER_TABLE', $parser->classifyStatement('ALTER TABLE t ADD COLUMN x TEXT /* DROP TABLE t */'));
    }

    public function testClassifyTclWithAlterTableInComment(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('TCL', $parser->classifyStatement('BEGIN /* ALTER TABLE t ADD COLUMN x TEXT */'));
    }

    public function testClassifyWithStatementStartAnchor(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('SELECT', $parser->classifyStatement("SELECT * FROM with_data"));
    }

    public function testExtractOnConflictColumnsLowercaseAs(): void
    {
        $parser = new PgSqlParser();
        $sql = "INSERT INTO users (id, name) VALUES (1, 'Alice') ON CONFLICT (id) DO UPDATE SET name = excluded.name";
        $result = $parser->extractOnConflictUpdateColumns($sql);
        self::assertSame(['name'], $result['columns']);
    }

    public function testExtractUpdateSetsAssignmentWithNewlineInValue(): void
    {
        $parser = new PgSqlParser();
        $sql = "UPDATE users SET name = 'line1\nline2' WHERE id = 1";
        $sets = $parser->extractUpdateSets($sql);
        self::assertArrayHasKey('name', $sets);
        self::assertStringContainsString('line1', $sets['name']);
    }

    public function testExtractWhereClauseLowercaseReturning(): void
    {
        $parser = new PgSqlParser();
        $sql = "UPDATE users SET name = 'Bob' WHERE id = 1 returning *";
        $where = $parser->extractWhereClause($sql);
        self::assertSame('id = 1', $where);
    }

    public function testExtractWhereClauseLowercaseOrderBy(): void
    {
        $parser = new PgSqlParser();
        $sql = 'DELETE FROM users WHERE active = false order by id';
        $where = $parser->extractWhereClause($sql);
        self::assertSame('active = false', $where);
    }

    public function testExtractWhereClauseLowercaseLimit(): void
    {
        $parser = new PgSqlParser();
        $sql = 'DELETE FROM users WHERE active = false limit 10';
        $where = $parser->extractWhereClause($sql);
        self::assertSame('active = false', $where);
    }

    public function testExtractDeleteUsingClauseLowercaseUsing(): void
    {
        $parser = new PgSqlParser();
        $sql = 'delete from users using orders where users.id = orders.user_id';
        $using = $parser->extractDeleteUsingClause($sql);
        self::assertSame('orders', $using);
    }

    public function testExtractSelectTableNamesWithMultilineJoin(): void
    {
        $parser = new PgSqlParser();
        $sql = "SELECT u.id\nFROM users u\nJOIN orders o ON u.id = o.user_id";
        $tables = $parser->extractSelectTableNames($sql);
        self::assertContains('users', $tables);
    }

    public function testExtractSelectTableNamesLowercaseFrom(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames('select id from users');
        self::assertContains('users', $tables);
    }

    public function testExtractOnConflictColumnsMultilineSetWithWhereFilter(): void
    {
        $parser = new PgSqlParser();
        $sql = "INSERT INTO users (id, name) VALUES (1, 'Alice') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name\nWHERE users.id > 0";
        $result = $parser->extractOnConflictUpdateColumns($sql);
        self::assertSame(['name'], $result['columns']);
    }

    public function testExtractSelectTableNamesCaseInsensitiveJoin(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames('select u.id from users u inner join orders o on u.id = o.user_id');
        self::assertContains('users', $tables);
        self::assertContains('orders', $tables);
    }

    public function testExtractDeleteUsingClauseWithNewlineBeforeWhere(): void
    {
        $parser = new PgSqlParser();
        $sql = "DELETE FROM users USING orders\nWHERE users.id = orders.user_id";
        $using = $parser->extractDeleteUsingClause($sql);
        self::assertNotNull($using);
        self::assertStringContainsString('orders', $using);
    }

    public function testClassifyDeleteContainingUpdateKeyword(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('DELETE', $parser->classifyStatement("DELETE FROM users WHERE action = 'UPDATE'"));
    }

    public function testClassifyCreateTableContainingSelectKeyword(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('CREATE_TABLE', $parser->classifyStatement("CREATE TABLE select_log (id INTEGER, query TEXT)"));
    }

    public function testHasInsertSelectCaseInsensitive(): void
    {
        $parser = new PgSqlParser();
        self::assertTrue($parser->hasInsertSelect('INSERT INTO archive SELECT * FROM users'));
        self::assertFalse($parser->hasInsertSelect("INSERT INTO users (id) VALUES (1)"));
    }

    public function testSplitStatementsSemicolonInsideLineComment(): void
    {
        $parser = new PgSqlParser();
        $stmts = $parser->splitStatements("SELECT 1 -- comment with ; semicolon\n; SELECT 2");
        self::assertCount(2, $stmts);
        self::assertStringContainsString('SELECT 1', $stmts[0]);
        self::assertStringContainsString('SELECT 2', $stmts[1]);
    }

    public function testClassifyStatementWithTwoLeadingLineComments(): void
    {
        $parser = new PgSqlParser();
        $sql = "-- first comment\n-- second comment\nSELECT 1";
        self::assertSame('SELECT', $parser->classifyStatement($sql));
    }

    public function testClassifyStatementLeadingLineAndBlockComments(): void
    {
        $parser = new PgSqlParser();
        $sql = "-- line comment\n/* block comment */\nSELECT 1";
        self::assertSame('SELECT', $parser->classifyStatement($sql));
    }

    public function testClassifyWithStatementQuotedIdentifierWithParen(): void
    {
        $parser = new PgSqlParser();
        $sql = 'WITH "cte(test)" AS (SELECT 1 AS id) SELECT * FROM "cte(test)"';
        self::assertSame('SELECT', $parser->classifyStatement($sql));
    }

    public function testExtractOnConflictUpdateColumnsWithLeadingWhitespace(): void
    {
        $parser = new PgSqlParser();
        $sql = "INSERT INTO t (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET  name  =  'b'  ";
        $result = $parser->extractOnConflictUpdateColumns($sql);
        self::assertSame(['name'], $result['columns']);
        self::assertSame('b', trim($result['values']['name'], "' "));
    }

    public function testExtractUpdateSetsWithLeadingWhitespace(): void
    {
        $parser = new PgSqlParser();
        $result = $parser->extractUpdateSets("UPDATE t SET  name  =  'Bob'  WHERE id = 1");
        self::assertSame(['name'], array_keys($result));
    }

    public function testExtractUpdateSetsWithNewlineInSetClause(): void
    {
        $parser = new PgSqlParser();
        $result = $parser->extractUpdateSets("UPDATE t SET name = 'Bob',\nemail = 'b@b.com' WHERE id = 1");
        self::assertSame(['name', 'email'], array_keys($result));
    }

    public function testExtractWhereClauseReturnsTrimmedResult(): void
    {
        $parser = new PgSqlParser();
        $where = $parser->extractWhereClause('UPDATE t SET name = 1 WHERE  id = 1  ');
        self::assertSame('id = 1', $where);
    }

    public function testExtractUpdateFromClauseReturnsTrimmedResult(): void
    {
        $parser = new PgSqlParser();
        $from = $parser->extractUpdateFromClause('UPDATE t SET name = o.name FROM  orders o  WHERE t.id = o.id');
        self::assertNotNull($from);
        self::assertSame('orders o', $from);
    }

    public function testExtractDeleteUsingClauseReturnsTrimmedResult(): void
    {
        $parser = new PgSqlParser();
        $using = $parser->extractDeleteUsingClause('DELETE FROM t USING  orders  WHERE t.id = orders.id');
        self::assertNotNull($using);
        self::assertSame('orders', $using);
    }

    public function testExtractCreateTableSelectSqlWithDollarAnchor(): void
    {
        $parser = new PgSqlParser();
        $sql = 'CREATE TABLE t AS SELECT id, name FROM users';
        $selectSql = $parser->extractCreateTableSelectSql($sql);
        self::assertSame('SELECT id, name FROM users', $selectSql);
    }

    public function testExtractSelectTableNamesFromClauseWithInnerJoin(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames('SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id');
        self::assertContains('users', $tables);
        self::assertContains('orders', $tables);
    }

    public function testExtractSelectTableNamesLowercaseAs(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames('SELECT * FROM users as u');
        self::assertContains('users', $tables);
        self::assertNotContains('u', $tables);
    }

    public function testExtractSelectTableNamesFromClauseStripLeftJoin(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames('SELECT * FROM users LEFT JOIN orders ON TRUE');
        self::assertContains('users', $tables);
        self::assertContains('orders', $tables);
    }

    public function testExtractSelectTableNamesSkipsSubquery(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames('SELECT * FROM (SELECT 1) AS sub');
        self::assertSame([], $tables);
    }

    public function testExtractSelectTableNamesSchemaQualifiedUnquoted(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames('SELECT * FROM public.users');
        self::assertContains('users', $tables);
    }

    public function testExtractSelectTableNamesSchemaQualifiedUnquotedDotQuoted(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames('SELECT * FROM public."users"');
        self::assertContains('users', $tables);
    }

    public function testStripSchemaPrefixQuoted(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('"users"', $parser->stripSchemaPrefix('"public"."users"'));
    }

    public function testStripSchemaPrefixUnquoted(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('users', $parser->stripSchemaPrefix('public.users'));
    }

    public function testIsSqlKeywordBlocksKeywordAsTableName(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames('SELECT * FROM select');
        self::assertNotContains('select', $tables);
    }

    public function testClassifySimpleStatementWithLeadingWhitespace(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('SELECT', $parser->classifyStatement('   SELECT 1'));
    }

    public function testExtractSelectTableNamesWithLeadingWhitespaceInPart(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames('SELECT * FROM  users ');
        self::assertContains('users', $tables);
    }

    public function testStripStringLiteralsHandlesEscapedString(): void
    {
        $parser = new PgSqlParser();
        $ref = new \ReflectionMethod($parser, 'stripStringLiterals');
        $stripped = $ref->invoke($parser, "SELECT E'hello\\'world'");
        self::assertIsString($stripped);
        self::assertStringNotContainsString('hello', $stripped);
    }

    public function testStripStringLiteralsDollarQuoted(): void
    {
        $parser = new PgSqlParser();
        $ref = new \ReflectionMethod($parser, 'stripStringLiterals');
        $stripped = $ref->invoke($parser, "SELECT \$\$hello world\$\$");
        self::assertIsString($stripped);
        self::assertStringNotContainsString('hello', $stripped);
    }

    public function testClassifyTclBEGINWithAlterTableComment(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('TCL', $parser->classifyStatement('BEGIN'));
    }

    public function testExtractOnConflictColumnsWithLowercaseDoUpdateSet(): void
    {
        $parser = new PgSqlParser();
        $sql = "INSERT INTO t (id, name) VALUES (1, 'a') on conflict (id) do update set name = 'b'";
        $result = $parser->extractOnConflictUpdateColumns($sql);
        self::assertSame(['name'], $result['columns']);
    }

    public function testExtractOnConflictColumnsMultilineAssignment(): void
    {
        $parser = new PgSqlParser();
        $sql = "INSERT INTO t (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET\nname = 'b'";
        $result = $parser->extractOnConflictUpdateColumns($sql);
        self::assertSame(['name'], $result['columns']);
    }

    public function testExtractUpdateSetsLowercaseKeywords(): void
    {
        $parser = new PgSqlParser();
        $result = $parser->extractUpdateSets("update t set name = 'Bob' where id = 1");
        self::assertSame(['name'], array_keys($result));
    }

    public function testExtractUpdateSetsMultilineSetToFrom(): void
    {
        $parser = new PgSqlParser();
        $result = $parser->extractUpdateSets("UPDATE t SET name = 'Bob'\nFROM other WHERE t.id = other.id");
        self::assertSame(['name'], array_keys($result));
    }

    public function testSplitStatementsPreservesSemicolonInsideSingleQuote(): void
    {
        $parser = new PgSqlParser();
        $stmts = $parser->splitStatements("SELECT 'a;b'; SELECT 2");
        self::assertCount(2, $stmts);
        self::assertStringContainsString("'a;b'", $stmts[0]);
    }

    public function testSplitStatementsPreservesSemicolonInsideDoubleQuote(): void
    {
        $parser = new PgSqlParser();
        $stmts = $parser->splitStatements('SELECT "col;name" FROM t; SELECT 2');
        self::assertCount(2, $stmts);
        self::assertStringContainsString('"col;name"', $stmts[0]);
    }

    public function testSplitStatementsPreservesSemicolonInsideDollarQuote(): void
    {
        $parser = new PgSqlParser();
        $stmts = $parser->splitStatements("SELECT \$\$a;b\$\$; SELECT 2");
        self::assertCount(2, $stmts);
    }

    public function testSplitStatementsPreservesSemicolonInsideBlockComment(): void
    {
        $parser = new PgSqlParser();
        $stmts = $parser->splitStatements("SELECT 1 /* comment ; here */; SELECT 2");
        self::assertCount(2, $stmts);
    }

    public function testExtractSelectTableNamesStripsCrossJoinFromClause(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames('SELECT * FROM users CROSS JOIN orders');
        self::assertContains('users', $tables);
        self::assertContains('orders', $tables);
    }

    public function testExtractSelectTableNamesStripsNaturalJoinFromClause(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames('SELECT * FROM users NATURAL JOIN orders');
        self::assertContains('users', $tables);
        self::assertContains('orders', $tables);
    }


    public function testExtractWhereClauseWithNewlineBeforeReturning(): void
    {
        $parser = new PgSqlParser();
        $where = $parser->extractWhereClause("DELETE FROM t WHERE id = 1\nRETURNING *");
        self::assertSame('id = 1', $where);
    }

    public function testHasCreateTableLikeLowercaseLike(): void
    {
        $parser = new PgSqlParser();
        self::assertTrue($parser->hasCreateTableLike('CREATE TABLE t (like other_table)'));
    }

    public function testMapStrippedOffsetWhenLengthsMatch(): void
    {
        $parser = new PgSqlParser();
        $where = $parser->extractWhereClause('DELETE FROM t WHERE id = 1');
        self::assertSame('id = 1', $where);
    }

    public function testMapStrippedOffsetWithStringLiteral(): void
    {
        $parser = new PgSqlParser();
        $where = $parser->extractWhereClause("DELETE FROM t WHERE name = 'hello' AND id = 1");
        self::assertNotNull($where);
        self::assertStringContainsString('hello', $where);
        self::assertStringContainsString('id = 1', $where);
    }

    public function testReturnRemovalFromMapStrippedOffsetToOriginal(): void
    {
        $parser = new PgSqlParser();
        $where = $parser->extractWhereClause("UPDATE t SET x = 1 WHERE name = 'foo' RETURNING id");
        self::assertNotNull($where);
        self::assertStringNotContainsString('RETURNING', $where);
    }

    public function testExtractSelectTableNamesEmptyTrimmedPartSkipped(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames('SELECT * FROM users');
        self::assertContains('users', $tables);
    }

    public function testClassifyStatementSETTransactionTCL(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('TCL', $parser->classifyStatement('SET TRANSACTION ISOLATION LEVEL SERIALIZABLE'));
    }

    public function testClassifyStatementStartTransaction(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('TCL', $parser->classifyStatement('START TRANSACTION'));
    }

    public function testExtractOnConflictColumnsValueWithNewline(): void
    {
        $parser = new PgSqlParser();
        $sql = "INSERT INTO t (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name\n|| ' updated'";
        $result = $parser->extractOnConflictUpdateColumns($sql);
        self::assertSame(['name'], $result['columns']);
        self::assertStringContainsString('updated', $result['values']['name']);
    }

    public function testExtractOnConflictColumnsAssignmentWithPaddedEquals(): void
    {
        $parser = new PgSqlParser();
        $sql = "INSERT INTO t (id, x) VALUES (1, 2) ON CONFLICT (id) DO UPDATE SET  x  =  3 ";
        $result = $parser->extractOnConflictUpdateColumns($sql);
        self::assertSame(['x'], $result['columns']);
        self::assertSame('3', $result['values']['x']);
    }

    public function testHasInsertSelectLowercaseKeywords(): void
    {
        $parser = new PgSqlParser();
        self::assertTrue($parser->hasInsertSelect('insert into archive select * from users'));
    }

    public function testHasInsertSelectWithNewlineBeforeSelect(): void
    {
        $parser = new PgSqlParser();
        self::assertTrue($parser->hasInsertSelect("INSERT INTO archive\nSELECT * FROM users"));
    }

    public function testHasInsertSelectWithValuesReturnsFalse(): void
    {
        $parser = new PgSqlParser();
        self::assertFalse($parser->hasInsertSelect("INSERT INTO t (id) VALUES (1)"));
    }

    public function testExtractUpdateSetsWithPaddedAssignment(): void
    {
        $parser = new PgSqlParser();
        $result = $parser->extractUpdateSets("UPDATE t SET  x  =  1  ,  y  =  2  WHERE id = 3");
        self::assertSame(['x', 'y'], array_keys($result));
        self::assertSame('1', $result['x']);
        self::assertSame('2', $result['y']);
    }

    public function testExtractUpdateSetsNewlineInValue(): void
    {
        $parser = new PgSqlParser();
        $result = $parser->extractUpdateSets("UPDATE t SET x = 1\n+ 2 WHERE id = 1");
        self::assertSame(['x'], array_keys($result));
    }

    public function testExtractWhereClauseLowercaseWhere(): void
    {
        $parser = new PgSqlParser();
        $where = $parser->extractWhereClause('delete from t where id = 1');
        self::assertSame('id = 1', $where);
    }

    public function testExtractUpdateFromClauseLowercaseFromAndSet(): void
    {
        $parser = new PgSqlParser();
        $from = $parser->extractUpdateFromClause('update t set name = o.name from orders o where t.id = o.id');
        self::assertNotNull($from);
        self::assertStringContainsString('orders', $from);
    }

    public function testExtractDeleteUsingClauseLowercaseUsingAndWhere(): void
    {
        $parser = new PgSqlParser();
        $using = $parser->extractDeleteUsingClause('delete from t using orders where t.id = orders.id');
        self::assertNotNull($using);
        self::assertStringContainsString('orders', $using);
    }

    public function testHasCreateTableLikeLowercaseKeywords(): void
    {
        $parser = new PgSqlParser();
        self::assertTrue($parser->hasCreateTableLike('create table t (like other_table)'));
    }

    public function testHasCreateTableLikeWithNewlineBeforeLike(): void
    {
        $parser = new PgSqlParser();
        self::assertTrue($parser->hasCreateTableLike("CREATE TABLE t (\nLIKE other_table)"));
    }

    public function testExtractCreateTableSelectSqlLowercaseAs(): void
    {
        $parser = new PgSqlParser();
        $sql = 'CREATE TABLE t as SELECT id FROM users';
        $selectSql = $parser->extractCreateTableSelectSql($sql);
        self::assertNotNull($selectSql);
        self::assertStringStartsWith('SELECT', $selectSql);
    }

    public function testExtractSelectTableNamesLowercaseJoin(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames('SELECT * FROM users join orders ON TRUE');
        self::assertContains('users', $tables);
        self::assertContains('orders', $tables);
    }

    public function testExtractSelectTableNamesLowercaseAlias(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames('SELECT * FROM users as u WHERE u.id = 1');
        self::assertContains('users', $tables);
    }

    public function testClassifySimpleStatementWithLtrimNeeded(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('INSERT', $parser->classifyStatement("\t  INSERT INTO t VALUES (1)"));
    }

    public function testSplitByTopLevelCommaTrimsResults(): void
    {
        $parser = new PgSqlParser();
        $result = $parser->extractUpdateSets("UPDATE t SET x = 1 , y = 2 WHERE id = 1");
        self::assertSame('1', $result['x']);
        self::assertSame('2', $result['y']);
    }

    public function testSplitByTopLevelCommaTrimsLastItem(): void
    {
        $parser = new PgSqlParser();
        $result = $parser->extractUpdateSets("UPDATE t SET x = 1 WHERE id = 1");
        self::assertSame('1', $result['x']);
    }

    public function testExtractSelectTableNamesWithFromClauseContainingLeftJoin(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames('SELECT * FROM users LEFT OUTER JOIN orders ON TRUE');
        self::assertContains('users', $tables);
        self::assertContains('orders', $tables);
    }

    public function testExtractSelectTableNamesWithLowercaseAs(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames('SELECT u.id FROM users as u');
        self::assertContains('users', $tables);
    }

    public function testClassifyTruncateContainsDeleteInComment(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('TRUNCATE', $parser->classifyStatement('TRUNCATE TABLE t'));
    }

    public function testExtractWhereClauseWithStringLiteralContainingWhere(): void
    {
        $parser = new PgSqlParser();
        $where = $parser->extractWhereClause("DELETE FROM t WHERE name = 'WHERE clause'");
        self::assertNotNull($where);
        self::assertStringContainsString("'WHERE clause'", $where);
    }

    public function testHasInsertSelectWithLowercaseValues(): void
    {
        $parser = new PgSqlParser();
        self::assertFalse($parser->hasInsertSelect("INSERT INTO t (id) values ((SELECT 1))"));
    }

    public function testExtractWhereClauseNewlineBeforeReturningKeyword(): void
    {
        $parser = new PgSqlParser();
        $where = $parser->extractWhereClause("UPDATE t SET x = 1 WHERE id = 5\nRETURNING *");
        self::assertNotNull($where);
        self::assertStringNotContainsString('RETURNING', $where);
        self::assertStringContainsString('id = 5', $where);
    }

    public function testExtractUpdateFromClauseNewlineBeforeWhere(): void
    {
        $parser = new PgSqlParser();
        $from = $parser->extractUpdateFromClause("UPDATE t SET x = s.x FROM src s\nWHERE t.id = s.id");
        self::assertNotNull($from);
        self::assertStringContainsString('src', $from);
        self::assertStringNotContainsString('WHERE', $from);
    }

    public function testExtractDeleteUsingClauseNewlineBeforeWhere(): void
    {
        $parser = new PgSqlParser();
        $using = $parser->extractDeleteUsingClause("DELETE FROM t USING src\nWHERE t.id = src.id");
        self::assertNotNull($using);
        self::assertStringContainsString('src', $using);
        self::assertStringNotContainsString('WHERE', $using);
    }

    public function testHasCreateTableLikeWithNewlineBetweenParenAndLike(): void
    {
        $parser = new PgSqlParser();
        self::assertTrue($parser->hasCreateTableLike("CREATE TABLE t (\nLIKE src)"));
    }

    public function testExtractTableRefsLowercaseAsKeyword(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames('SELECT u.id FROM users as u');
        self::assertContains('users', $tables);
        self::assertNotContains('u', $tables);
    }

    public function testExtractUpdateFromClauseTrimmedResult(): void
    {
        $parser = new PgSqlParser();
        $from = $parser->extractUpdateFromClause("UPDATE t SET x = s.x FROM  src s  WHERE t.id = s.id");
        self::assertNotNull($from);
        self::assertSame('src s', $from);
    }

    public function testExtractDeleteUsingClauseTrimmedOutput(): void
    {
        $parser = new PgSqlParser();
        $using = $parser->extractDeleteUsingClause("DELETE FROM t USING  src s  WHERE t.id = src.id");
        self::assertNotNull($using);
        self::assertSame('src s', $using);
    }

    public function testExtractUpdateSetsValueWithTrailingNewline(): void
    {
        $parser = new PgSqlParser();
        $sets = $parser->extractUpdateSets("UPDATE t SET x = 1\n");
        self::assertNotEmpty($sets);
        self::assertSame('1', $sets['x']);
    }

    public function testExtractOnConflictUpdateColumnsValueTrimmed(): void
    {
        $parser = new PgSqlParser();
        $info = $parser->extractOnConflictUpdateColumns("INSERT INTO t (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name ");
        self::assertNotEmpty($info['columns']);
        self::assertSame('EXCLUDED.name', $info['values']['name']);
    }

    public function testClassifyStatementWithInternalWithKeyword(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('SELECT', $parser->classifyStatement("SELECT * FROM t WHERE name = 'WITH'"));
    }

    public function testClassifyStatementTruncateContainsTclKeywordInBody(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('TRUNCATE', $parser->classifyStatement('TRUNCATE TABLE begin_backup'));
    }

    public function testStripSchemaPrefixUnquotedWithTrailingDot(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('"users"', $parser->stripSchemaPrefix('public."users"'));
    }

    public function testExtractCreateTableSelectSqlDollarRemovedMatchesCorrectly(): void
    {
        $parser = new PgSqlParser();
        $selectSql = $parser->extractCreateTableSelectSql('CREATE TABLE t AS SELECT 1 AS id');
        self::assertNotNull($selectSql);
        self::assertSame('SELECT 1 AS id', $selectSql);
    }

    public function testUnquoteIdentifierWithOnlyStartingQuote(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('"test', $parser->unquoteIdentifier('"test'));
    }

    public function testUnquoteIdentifierWithOnlyEndingQuote(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('test"', $parser->unquoteIdentifier('test"'));
    }

    public function testExtractSelectTableNamesFromWithGreaterThanZero(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames('SELECT 1');
        self::assertSame([], $tables);
    }

    public function testClassifyWithStatementDepthNeverNegative(): void
    {
        $parser = new PgSqlParser();
        $result = $parser->classifyStatement('WITH cte AS (SELECT (1)) SELECT * FROM cte');
        self::assertSame('SELECT', $result);
    }

    public function testExtractUpdateSetsAssignmentValueTrimmed(): void
    {
        $parser = new PgSqlParser();
        $sets = $parser->extractUpdateSets("UPDATE t SET name =  'Alice'  ");
        self::assertNotEmpty($sets);
        self::assertSame("'Alice'", $sets['name']);
    }

    public function testStripSchemaPrefixQuotedWithDollarAnchor(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('users', $parser->stripSchemaPrefix('"public".users'));
    }

    public function testStripSchemaPrefixUnquotedDollarAnchor(): void
    {
        $parser = new PgSqlParser();
        self::assertSame('users', $parser->stripSchemaPrefix('public.users'));
    }

    public function testExtractOnConflictUpdateColumnsAssignmentPregDollar(): void
    {
        $parser = new PgSqlParser();
        $info = $parser->extractOnConflictUpdateColumns("INSERT INTO t (id, x) VALUES (1, 2) ON CONFLICT (id) DO UPDATE SET x = t.x + 1");
        self::assertContains('x', $info['columns']);
        self::assertSame('t.x + 1', $info['values']['x']);
    }

    public function testExtractUpdateSetsPregDollarAnchor(): void
    {
        $parser = new PgSqlParser();
        $sets = $parser->extractUpdateSets("UPDATE t SET x = y + 1");
        self::assertNotEmpty($sets);
        self::assertSame('y + 1', $sets['x']);
    }

    public function testSplitStatementsSubtractionNotTreatedAsComment(): void
    {
        $parser = new PgSqlParser();
        $stmts = $parser->splitStatements('SELECT 5-3; SELECT 1');
        self::assertCount(2, $stmts);
        self::assertSame('SELECT 5-3', $stmts[0]);
        self::assertSame('SELECT 1', $stmts[1]);
    }

    public function testSplitStatementsDivisionNotTreatedAsBlockComment(): void
    {
        $parser = new PgSqlParser();
        $stmts = $parser->splitStatements('SELECT 6/2; SELECT 1');
        self::assertCount(2, $stmts);
        self::assertSame('SELECT 6/2', $stmts[0]);
        self::assertSame('SELECT 1', $stmts[1]);
    }

    public function testExtractInsertValuesCommaInsideSingleQuote(): void
    {
        $parser = new PgSqlParser();
        $rows = $parser->extractInsertValues("INSERT INTO t (name, age) VALUES ('hello, world', 5)");
        self::assertCount(1, $rows);
        self::assertCount(2, $rows[0]);
        self::assertSame("'hello, world'", $rows[0][0]);
        self::assertSame('5', $rows[0][1]);
    }

    public function testHasInsertSelectWithSubqueryInValues(): void
    {
        $parser = new PgSqlParser();
        self::assertFalse($parser->hasInsertSelect('INSERT INTO t (id) VALUES ((SELECT 1))'));
    }

    public function testExtractUpdateSetsCommaInSingleQuotedValue(): void
    {
        $parser = new PgSqlParser();
        $sets = $parser->extractUpdateSets("UPDATE t SET name = 'a, b', age = 5");
        self::assertCount(2, $sets);
        self::assertSame("'a, b'", $sets['name']);
        self::assertSame('5', $sets['age']);
    }

    public function testExtractOnConflictUpdateColumnsCommaInValue(): void
    {
        $parser = new PgSqlParser();
        $info = $parser->extractOnConflictUpdateColumns("INSERT INTO t (id, name) VALUES (1, 'x') ON CONFLICT (id) DO UPDATE SET name = 'hello, world'");
        self::assertContains('name', $info['columns']);
        self::assertSame("'hello, world'", $info['values']['name']);
    }

    public function testExtractSelectTableNamesUnionMultipleFromClauses(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames('SELECT * FROM users UNION SELECT * FROM orders');
        self::assertContains('users', $tables);
        self::assertContains('orders', $tables);
    }

    public function testExtractSelectTableNamesCommaInDoubleQuotedIdentifier(): void
    {
        $parser = new PgSqlParser();
        $tables = $parser->extractSelectTableNames('SELECT * FROM users, orders');
        self::assertContains('users', $tables);
        self::assertContains('orders', $tables);
    }

    public function testSplitByTopLevelCommaDoubleQuotedStringWithComma(): void
    {
        $parser = new PgSqlParser();
        $sets = $parser->extractUpdateSets('UPDATE t SET "a,b" = 1, c = 2');
        self::assertCount(2, $sets);
        self::assertSame('1', $sets['a,b']);
        self::assertSame('2', $sets['c']);
    }

    public function testExtractOnConflictColumnsCommaInDoubleQuotedCol(): void
    {
        $parser = new PgSqlParser();
        $info = $parser->extractOnConflictUpdateColumns('INSERT INTO t (id, "a,b") VALUES (1, 2) ON CONFLICT (id) DO UPDATE SET "a,b" = EXCLUDED."a,b"');
        self::assertContains('a,b', $info['columns']);
    }

    public function testSplitStatementsAsteriskNotTreatedAsBlockComment(): void
    {
        $parser = new PgSqlParser();
        $stmts = $parser->splitStatements('SELECT *FROM t; SELECT 1');
        self::assertCount(2, $stmts);
    }

    public function testExtractInsertValuesCommaInsideDoubleQuotedDefault(): void
    {
        $parser = new PgSqlParser();
        $rows = $parser->extractInsertValues("INSERT INTO t (a, b) VALUES (1, 'x,y')");
        self::assertCount(1, $rows);
        self::assertCount(2, $rows[0]);
    }
}
