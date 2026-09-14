<?php

declare(strict_types=1);

namespace Tests\Unit\Rewrite;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;
use ZtdQuery\Platform\Postgres\Rewrite\PgSqlQueryGuard;
use ZtdQuery\Platform\Postgres\Sql\PgSqlParser;
use ZtdQuery\Rewrite\QueryKind;

#[CoversClass(PgSqlQueryGuard::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Diagnostic\PgSqlReadOnlyDiagnosticStatement::class)]
#[UsesClass(PgSqlParser::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\PostgreSqlLexicalMasker::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Conflict\ColumnSet::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Diagnostic\KeywordSearch::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Relation\FromClause::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Relation\RelationReference::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Statement\Classification::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Statement\ConflictClause::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Statement\Identifiers::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Statement\InsertSource::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Statement\TableDefinitionClauses::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Conflict\PgSqlConflictTarget::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Relation\PgSqlSelectRelationParser::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Lexing\CommentSpan::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Lexing\QuotedSpan::class)]
final class PgSqlQueryGuardTest extends \PHPUnit\Framework\TestCase
{
    public function testSelectClassifiesAsRead(): void
    {
        $guard = new PgSqlQueryGuard(new PgSqlParser());
        self::assertSame(QueryKind::READ, $guard->classify('SELECT * FROM users'));
    }

    public function testInsertClassifiesAsWriteSimulated(): void
    {
        $guard = new PgSqlQueryGuard(new PgSqlParser());
        self::assertSame(QueryKind::WRITE_SIMULATED, $guard->classify("INSERT INTO users (id, name) VALUES (1, 'Alice')"));
    }

    public function testUpdateClassifiesAsWriteSimulated(): void
    {
        $guard = new PgSqlQueryGuard(new PgSqlParser());
        self::assertSame(QueryKind::WRITE_SIMULATED, $guard->classify("UPDATE users SET name = 'Bob' WHERE id = 1"));
    }

    public function testDeleteClassifiesAsWriteSimulated(): void
    {
        $guard = new PgSqlQueryGuard(new PgSqlParser());
        self::assertSame(QueryKind::WRITE_SIMULATED, $guard->classify('DELETE FROM users WHERE id = 1'));
    }

    public function testCreateTableClassifiesAsDdlSimulated(): void
    {
        $guard = new PgSqlQueryGuard(new PgSqlParser());
        self::assertSame(QueryKind::DDL_SIMULATED, $guard->classify('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)'));
    }

    public function testDropTableClassifiesAsDdlSimulated(): void
    {
        $guard = new PgSqlQueryGuard(new PgSqlParser());
        self::assertSame(QueryKind::DDL_SIMULATED, $guard->classify('DROP TABLE users'));
    }

    public function testGarbageInputReturnsNull(): void
    {
        self::assertNull((new PgSqlQueryGuard(new PgSqlParser()))->classify('NOT VALID SQL %%% @@@'), 'Garbage input should return null if it is not refused');
    }
    public function testMergeClassifiesAsWriteSimulated(): void
    {
        $guard = new PgSqlQueryGuard(new PgSqlParser());
        self::assertSame(QueryKind::WRITE_SIMULATED, $guard->classify('MERGE INTO users USING source ON users.id = source.id WHEN MATCHED THEN DELETE'));
    }
    public function testTruncateClassifiesAsWriteSimulated(): void
    {
        $guard = new PgSqlQueryGuard(new PgSqlParser());
        self::assertSame(QueryKind::WRITE_SIMULATED, $guard->classify('TRUNCATE TABLE users'));
    }
    public function testAlterTableClassifiesAsDdlSimulated(): void
    {
        $guard = new PgSqlQueryGuard(new PgSqlParser());
        self::assertSame(QueryKind::DDL_SIMULATED, $guard->classify('ALTER TABLE users ADD COLUMN email TEXT'));
    }
    public function testBeginClassifiesAsSkipped(): void
    {
        $guard = new PgSqlQueryGuard(new PgSqlParser());
        self::assertSame(QueryKind::SKIPPED, $guard->classify('BEGIN'));
    }
    public function testCommitClassifiesAsSkipped(): void
    {
        $guard = new PgSqlQueryGuard(new PgSqlParser());
        self::assertSame(QueryKind::SKIPPED, $guard->classify('COMMIT'));
    }
    public function testRollbackClassifiesAsSkipped(): void
    {
        $guard = new PgSqlQueryGuard(new PgSqlParser());
        self::assertSame(QueryKind::SKIPPED, $guard->classify('ROLLBACK'));
    }
    public function testUnsupportedReturnsNull(): void
    {
        $guard = new PgSqlQueryGuard(new PgSqlParser());
        self::assertNull($guard->classify('CREATE DATABASE test'));
    }
    public function testDoBlockClassifiesAsPassthroughRead(): void
    {
        $guard = new PgSqlQueryGuard(new PgSqlParser());
        self::assertSame(QueryKind::READ, $guard->classify('DO $$ BEGIN INSERT INTO users VALUES (1); END $$'));
    }
    public function testWithSelectClassifiesAsRead(): void
    {
        $guard = new PgSqlQueryGuard(new PgSqlParser());
        self::assertSame(QueryKind::READ, $guard->classify('WITH cte AS (SELECT 1) SELECT * FROM cte'));
    }
    public function testWithInsertClassifiesAsWriteSimulated(): void
    {
        $guard = new PgSqlQueryGuard(new PgSqlParser());
        self::assertSame(QueryKind::WRITE_SIMULATED, $guard->classify('WITH vals AS (SELECT 1 AS id) INSERT INTO users SELECT * FROM vals'));
    }
    public function testGarbageReturnsNull(): void
    {
        $guard = new PgSqlQueryGuard(new PgSqlParser());
        self::assertNull($guard->classify('GIBBERISH NONSENSE'));
    }
    public function testCreateTemporaryTableClassifiesAsDdlSimulated(): void
    {
        $guard = new PgSqlQueryGuard(new PgSqlParser());
        self::assertSame(QueryKind::DDL_SIMULATED, $guard->classify('CREATE TEMPORARY TABLE tmp (id INTEGER)'));
    }
    public function testDropTableIfExistsClassifiesAsDdlSimulated(): void
    {
        $guard = new PgSqlQueryGuard(new PgSqlParser());
        self::assertSame(QueryKind::DDL_SIMULATED, $guard->classify('DROP TABLE IF EXISTS users'));
    }
    public function testEmptyStringReturnsNull(): void
    {
        $guard = new PgSqlQueryGuard(new PgSqlParser());
        self::assertNull($guard->classify(''));
    }
    public function testSetCommandReturnsNull(): void
    {
        $guard = new PgSqlQueryGuard(new PgSqlParser());
        self::assertNull($guard->classify('SET search_path TO public'));
    }
    public function testShowCommandClassifiesAsRead(): void
    {
        $guard = new PgSqlQueryGuard(new PgSqlParser());
        self::assertSame(QueryKind::READ, $guard->classify('SHOW server_version'));
    }
    public function testSavepointClassifiesAsSkipped(): void
    {
        $guard = new PgSqlQueryGuard(new PgSqlParser());
        self::assertSame(QueryKind::SKIPPED, $guard->classify('SAVEPOINT sp1'));
    }
    public function testReleaseSavepointClassifiesAsSkipped(): void
    {
        $guard = new PgSqlQueryGuard(new PgSqlParser());
        self::assertSame(QueryKind::SKIPPED, $guard->classify('RELEASE SAVEPOINT sp1'));
    }
    public function testNullReturnFromClassifyIsDistinctFromSkipped(): void
    {
        $guard = new PgSqlQueryGuard(new PgSqlParser());
        $result = $guard->classify('GRANT ALL ON users TO admin');
        self::assertNull($result);
        self::assertNotSame(QueryKind::SKIPPED, $result);
    }
    public function testClassifiesSafeExplainAndRejectsExecutingWrite(): void
    {
        $guard = new PgSqlQueryGuard(new PgSqlParser());
        self::assertSame(QueryKind::READ, $guard->classify('EXPLAIN SELECT 1'));
        self::assertSame(QueryKind::READ, $guard->classify('EXPLAIN UPDATE users SET active = FALSE'));
        self::assertSame(QueryKind::READ, $guard->classify('EXPLAIN (ANALYZE TRUE, FORMAT JSON) SELECT 1'));
        self::assertNull($guard->classify('EXPLAIN ANALYZE UPDATE users SET active = FALSE'));
    }
    public function testClassifySelectLowercaseIsRead(): void
    {
        $guard = new PgSqlQueryGuard(new PgSqlParser());
        self::assertSame(QueryKind::READ, $guard->classify('select * from users'));
    }
    public function testClassifySetTransactionIsSkipped(): void
    {
        $guard = new PgSqlQueryGuard(new PgSqlParser());
        self::assertSame(QueryKind::SKIPPED, $guard->classify('SET TRANSACTION ISOLATION LEVEL SERIALIZABLE'));
    }
    public function testClassifyStartTransactionIsSkipped(): void
    {
        $guard = new PgSqlQueryGuard(new PgSqlParser());
        self::assertSame(QueryKind::SKIPPED, $guard->classify('START TRANSACTION'));
    }
    public function testClassifyWithSelectIsRead(): void
    {
        $guard = new PgSqlQueryGuard(new PgSqlParser());
        self::assertSame(QueryKind::READ, $guard->classify('WITH cte AS (SELECT 1) SELECT * FROM cte'));
    }
    public function testClassifyWithDeleteIsWrite(): void
    {
        $guard = new PgSqlQueryGuard(new PgSqlParser());
        self::assertSame(QueryKind::WRITE_SIMULATED, $guard->classify('WITH old AS (SELECT id FROM users) DELETE FROM users WHERE id IN (SELECT id FROM old)'));
    }
    public function testClassifyAlterTableLowercaseIsDdl(): void
    {
        $guard = new PgSqlQueryGuard(new PgSqlParser());
        self::assertSame(QueryKind::DDL_SIMULATED, $guard->classify('alter table users add column email text'));
    }
    public function testClassifyDropTableLowercaseIsDdl(): void
    {
        $guard = new PgSqlQueryGuard(new PgSqlParser());
        self::assertSame(QueryKind::DDL_SIMULATED, $guard->classify('drop table users'));
    }
    public function testClassifyTruncateLowercaseIsWrite(): void
    {
        $guard = new PgSqlQueryGuard(new PgSqlParser());
        self::assertSame(QueryKind::WRITE_SIMULATED, $guard->classify('truncate table users'));
    }
}
