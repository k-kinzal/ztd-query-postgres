<?php

declare(strict_types=1);

namespace Tests\Unit;

use Tests\Contract\QueryClassifierContractTest;
use ZtdQuery\Platform\Postgres\PgSqlParser;
use ZtdQuery\Platform\Postgres\PgSqlQueryGuard;
use ZtdQuery\Rewrite\QueryKind;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;

#[CoversClass(PgSqlQueryGuard::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlReadOnlyDiagnosticStatement::class)]
#[UsesClass(PgSqlParser::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PostgreSqlLexicalMasker::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlLexerProfile::class)]
final class PgSqlQueryGuardTest extends QueryClassifierContractTest
{
    protected function classify(string $sql): ?QueryKind
    {
        return (new PgSqlQueryGuard(new PgSqlParser()))->classify($sql);
    }

    protected function selectSql(): string
    {
        return 'SELECT * FROM users';
    }

    protected function insertSql(): string
    {
        return "INSERT INTO users (name) VALUES ('Alice')";
    }

    protected function updateSql(): string
    {
        return "UPDATE users SET name = 'Bob' WHERE id = 1";
    }

    protected function deleteSql(): string
    {
        return 'DELETE FROM users WHERE id = 1';
    }

    protected function createTableSql(): string
    {
        return 'CREATE TABLE test (id INTEGER PRIMARY KEY)';
    }

    protected function dropTableSql(): string
    {
        return 'DROP TABLE test';
    }

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

    public function testMergeClassifiesAsWriteSimulated(): void
    {
        $guard = new PgSqlQueryGuard(new PgSqlParser());

        self::assertSame(QueryKind::WRITE_SIMULATED, $guard->classify(
            'MERGE INTO users USING source ON users.id = source.id WHEN MATCHED THEN DELETE',
        ));
    }

    public function testTruncateClassifiesAsWriteSimulated(): void
    {
        $guard = new PgSqlQueryGuard(new PgSqlParser());
        self::assertSame(QueryKind::WRITE_SIMULATED, $guard->classify('TRUNCATE TABLE users'));
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

        self::assertSame(
            QueryKind::READ,
            $guard->classify("DO \$\$ BEGIN INSERT INTO users VALUES (1); END \$\$"),
        );
    }

    public function testWithSelectClassifiesAsRead(): void
    {
        $guard = new PgSqlQueryGuard(new PgSqlParser());
        self::assertSame(QueryKind::READ, $guard->classify('WITH cte AS (SELECT 1) SELECT * FROM cte'));
    }

    public function testWithInsertClassifiesAsWriteSimulated(): void
    {
        $guard = new PgSqlQueryGuard(new PgSqlParser());
        self::assertSame(QueryKind::WRITE_SIMULATED, $guard->classify("WITH vals AS (SELECT 1 AS id) INSERT INTO users SELECT * FROM vals"));
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
