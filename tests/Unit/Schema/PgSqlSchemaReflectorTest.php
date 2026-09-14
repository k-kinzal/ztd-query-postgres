<?php

declare(strict_types=1);

namespace Tests\Unit\Schema;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Connection\ConnectionInterface;
use ZtdQuery\Connection\StatementInterface;
use ZtdQuery\Platform\Postgres\Schema\PgSqlSchemaReflector;
use ZtdQuery\Platform\Postgres\Sql\PgSqlIdentifierQuoter;

#[CoversClass(PgSqlSchemaReflector::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Relation\PgSqlSelectRelationParser::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Schema\View\PgSqlViewDefinitionParser::class)]
#[UsesClass(PgSqlIdentifierQuoter::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::class)]
#[CoversClass(\ZtdQuery\Platform\Postgres\Schema\Reflection\Catalog\PartitionKeys::class)]
#[CoversClass(\ZtdQuery\Platform\Postgres\Schema\Reflection\Catalog\TableQueries::class)]
#[CoversClass(\ZtdQuery\Platform\Postgres\Schema\Reflection\Column\ColumnDefinitionSql::class)]
#[CoversClass(\ZtdQuery\Platform\Postgres\Schema\Reflection\Column\NativeTypeSql::class)]
#[CoversClass(\ZtdQuery\Platform\Postgres\Schema\Reflection\Key\ForeignKeyRow::class)]
#[CoversClass(\ZtdQuery\Platform\Postgres\Schema\Reflection\Key\ForeignKeys::class)]
#[CoversClass(\ZtdQuery\Platform\Postgres\Schema\Reflection\Key\IndexDefinitions::class)]
#[CoversClass(\ZtdQuery\Platform\Postgres\Schema\Reflection\Key\IndexRows::class)]
#[CoversClass(\ZtdQuery\Platform\Postgres\Schema\Reflection\Key\PrimaryColumns::class)]
#[CoversClass(\ZtdQuery\Platform\Postgres\Schema\Reflection\Key\UniqueIndexes::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Relation\FromClause::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Relation\RelationReference::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Partition\PgSqlPartitionParser::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Partition\BoundPredicate::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Partition\ClauseTokens::class)]
final class PgSqlSchemaReflectorTest extends TestCase
{
    public function testReflectViewsReturnsEmptyWhenQueryFails(): void
    {
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturn(false);
        self::assertSame([], (new PgSqlSchemaReflector($connection))->reflectViews());
    }
    public function testReflectViewsSkipsMalformedRows(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['viewname' => null, 'definition' => 'SELECT 1'], ['viewname' => '', 'definition' => 'SELECT 1'], ['viewname' => 'missing_query'], ['viewname' => 'non_string', 'definition' => null], ['viewname' => 'blank', 'definition' => '   '], ['viewname' => 'active_users', 'definition' => 'SELECT * FROM public.users WHERE active'], ['viewname' => 'all_users', 'definition' => 'SELECT * FROM public.users']]);
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturn($statement);
        $definitions = (new PgSqlSchemaReflector($connection))->reflectViews();
        self::assertSame(['active_users', 'all_users'], array_keys($definitions));
        self::assertSame(['users'], $definitions['active_users']->dependencies);
    }
    public function testGetCreateStatementReturnsNullWhenNoColumns(): void
    {
        $colStmt = static::createStub(StatementInterface::class);
        $colStmt->method('fetchAll')->willReturn([]);
        $connection = static::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturn($colStmt);
        $reflector = new PgSqlSchemaReflector($connection);
        self::assertNull($reflector->getCreateStatement('empty_table'));
    }
    public function testGetCreateStatementReturnsNullWhenQueryFails(): void
    {
        $connection = static::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturn(false);
        $reflector = new PgSqlSchemaReflector($connection);
        self::assertNull($reflector->getCreateStatement('nonexistent'));
    }
    public function testExactSqlForIntegerNotNull(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'id', 'data_type' => 'integer', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'NO', 'column_default' => null, 'udt_name' => 'int4']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([]);
        $statementResults = [$statement, $statement2, $statement3];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $r = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"t\" (\n  \"id\" INTEGER NOT NULL\n)", $r->getCreateStatement('t'));
    }
    public function testExactSqlForSmallint(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'v', 'data_type' => 'smallint', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'int2']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([]);
        $statementResults = [$statement, $statement2, $statement3];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $r = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"t\" (\n  \"v\" SMALLINT\n)", $r->getCreateStatement('t'));
    }
    public function testExactSqlForBigint(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'b', 'data_type' => 'bigint', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'int8']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([]);
        $statementResults = [$statement, $statement2, $statement3];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $r = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"t\" (\n  \"b\" BIGINT\n)", $r->getCreateStatement('t'));
    }
    public function testExactSqlForReal(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'f', 'data_type' => 'real', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'float4']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([]);
        $statementResults = [$statement, $statement2, $statement3];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $r = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"t\" (\n  \"f\" REAL\n)", $r->getCreateStatement('t'));
    }
    public function testExactSqlForDoublePrecision(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'd', 'data_type' => 'double precision', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'float8']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([]);
        $statementResults = [$statement, $statement2, $statement3];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $r = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"t\" (\n  \"d\" DOUBLE PRECISION\n)", $r->getCreateStatement('t'));
    }
    public function testExactSqlForBoolean(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'a', 'data_type' => 'boolean', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'bool']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([]);
        $statementResults = [$statement, $statement2, $statement3];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $r = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"t\" (\n  \"a\" BOOLEAN\n)", $r->getCreateStatement('t'));
    }
    public function testExactSqlForDate(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'x', 'data_type' => 'date', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'date']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([]);
        $statementResults = [$statement, $statement2, $statement3];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $r = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" DATE\n)", $r->getCreateStatement('t'));
    }
    public function testExactSqlForTimestamp(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'x', 'data_type' => 'timestamp without time zone', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'timestamp']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([]);
        $statementResults = [$statement, $statement2, $statement3];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $r = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" TIMESTAMP\n)", $r->getCreateStatement('t'));
    }
    public function testExactSqlForTimestamptz(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'x', 'data_type' => 'timestamp with time zone', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'timestamptz']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([]);
        $statementResults = [$statement, $statement2, $statement3];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $r = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" TIMESTAMPTZ\n)", $r->getCreateStatement('t'));
    }
    public function testExactSqlForTime(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'x', 'data_type' => 'time without time zone', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'time']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([]);
        $statementResults = [$statement, $statement2, $statement3];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $r = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" TIME\n)", $r->getCreateStatement('t'));
    }
    public function testExactSqlForTimetz(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'x', 'data_type' => 'time with time zone', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'timetz']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([]);
        $statementResults = [$statement, $statement2, $statement3];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $r = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" TIMETZ\n)", $r->getCreateStatement('t'));
    }
    public function testExactSqlForText(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'x', 'data_type' => 'text', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'text']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([]);
        $statementResults = [$statement, $statement2, $statement3];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $r = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" TEXT\n)", $r->getCreateStatement('t'));
    }
    public function testExactSqlForBytea(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'x', 'data_type' => 'bytea', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'bytea']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([]);
        $statementResults = [$statement, $statement2, $statement3];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $r = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" BYTEA\n)", $r->getCreateStatement('t'));
    }
    public function testExactSqlForJson(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'x', 'data_type' => 'json', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'json']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([]);
        $statementResults = [$statement, $statement2, $statement3];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $r = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" JSON\n)", $r->getCreateStatement('t'));
    }
    public function testExactSqlForJsonb(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'x', 'data_type' => 'jsonb', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'jsonb']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([]);
        $statementResults = [$statement, $statement2, $statement3];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $r = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" JSONB\n)", $r->getCreateStatement('t'));
    }
    public function testExactSqlForUuid(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'x', 'data_type' => 'uuid', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'uuid']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([]);
        $statementResults = [$statement, $statement2, $statement3];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $r = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" UUID\n)", $r->getCreateStatement('t'));
    }
    public function testExactSqlForVarcharWithLen(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'x', 'data_type' => 'character varying', 'character_maximum_length' => 50, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'varchar']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([]);
        $statementResults = [$statement, $statement2, $statement3];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $r = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" VARCHAR(50)\n)", $r->getCreateStatement('t'));
    }
    public function testExactSqlForVarcharNoLen(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'x', 'data_type' => 'character varying', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'varchar']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([]);
        $statementResults = [$statement, $statement2, $statement3];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $r = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" VARCHAR\n)", $r->getCreateStatement('t'));
    }
    public function testExactSqlForCharWithLen(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'x', 'data_type' => 'character', 'character_maximum_length' => 5, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'bpchar']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([]);
        $statementResults = [$statement, $statement2, $statement3];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $r = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" CHAR(5)\n)", $r->getCreateStatement('t'));
    }
    public function testExactSqlForCharNoLen(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'x', 'data_type' => 'character', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'bpchar']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([]);
        $statementResults = [$statement, $statement2, $statement3];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $r = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" CHAR(1)\n)", $r->getCreateStatement('t'));
    }
    public function testExactSqlPreservesBitWidths(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'fixed', 'data_type' => 'bit', 'character_maximum_length' => 8, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'bit'], ['column_name' => 'varying', 'data_type' => 'bit varying', 'character_maximum_length' => '16', 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'varbit']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([]);
        $statementResults = [$statement, $statement2, $statement3];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $r = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"t\" (\n  \"fixed\" BIT(8),\n  \"varying\" BIT VARYING(16)\n)", $r->getCreateStatement('t'));
    }
    public function testExactSqlPreservesUnboundedBitTypes(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'fixed', 'data_type' => 'bit', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'bit'], ['column_name' => 'varying', 'data_type' => 'bit varying', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'varbit']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([]);
        $statementResults = [$statement, $statement2, $statement3];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $r = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"t\" (\n  \"fixed\" BIT,\n  \"varying\" BIT VARYING\n)", $r->getCreateStatement('t'));
    }
    public function testExactSqlForNumericPrecisionScale(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'x', 'data_type' => 'numeric', 'character_maximum_length' => null, 'numeric_precision' => 10, 'numeric_scale' => 2, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'numeric']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([]);
        $statementResults = [$statement, $statement2, $statement3];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $r = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" NUMERIC(10,2)\n)", $r->getCreateStatement('t'));
    }
    public function testExactSqlForNumericPrecisionOnly(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'x', 'data_type' => 'numeric', 'character_maximum_length' => null, 'numeric_precision' => 18, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'numeric']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([]);
        $statementResults = [$statement, $statement2, $statement3];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $r = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" NUMERIC(18)\n)", $r->getCreateStatement('t'));
    }
    public function testExactSqlForNumericBare(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'x', 'data_type' => 'numeric', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'numeric']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([]);
        $statementResults = [$statement, $statement2, $statement3];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $r = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" NUMERIC\n)", $r->getCreateStatement('t'));
    }
    public function testExactSqlForUserDefinedCitext(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'x', 'data_type' => 'USER-DEFINED', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'citext']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([]);
        $statementResults = [$statement, $statement2, $statement3];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $r = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" CITEXT\n)", $r->getCreateStatement('t'));
    }
    public function testExactSqlForUserDefinedHstore(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'x', 'data_type' => 'USER-DEFINED', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'hstore']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([]);
        $statementResults = [$statement, $statement2, $statement3];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $r = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" HSTORE\n)", $r->getCreateStatement('t'));
    }
    public function testExactSqlForUserDefinedLtree(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'x', 'data_type' => 'USER-DEFINED', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'ltree']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([]);
        $statementResults = [$statement, $statement2, $statement3];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $r = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" LTREE\n)", $r->getCreateStatement('t'));
    }
    public function testExactSqlForUserDefinedCustom(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'x', 'data_type' => 'USER-DEFINED', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'myenum']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([]);
        $statementResults = [$statement, $statement2, $statement3];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $r = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" MYENUM\n)", $r->getCreateStatement('t'));
    }
    public function testExactSqlForUserDefinedEmpty(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'x', 'data_type' => 'USER-DEFINED', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => '']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([]);
        $statementResults = [$statement, $statement2, $statement3];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $r = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" TEXT\n)", $r->getCreateStatement('t'));
    }
    public function testExactSqlForArrayWithUnderscore(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'x', 'data_type' => 'ARRAY', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => '_int4']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([]);
        $statementResults = [$statement, $statement2, $statement3];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $r = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" INT4[]\n)", $r->getCreateStatement('t'));
    }
    public function testExactSqlForArrayNoUnderscore(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'x', 'data_type' => 'ARRAY', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'mytypes']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([]);
        $statementResults = [$statement, $statement2, $statement3];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $r = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" MYTYPES[]\n)", $r->getCreateStatement('t'));
    }
    public function testExactSqlWithDefault(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 's', 'data_type' => 'text', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'NO', 'column_default' => "'active'::text", 'udt_name' => 'text']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([]);
        $statementResults = [$statement, $statement2, $statement3];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $r = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"t\" (\n  \"s\" TEXT NOT NULL DEFAULT 'active'::text\n)", $r->getCreateStatement('t'));
    }
    public function testExactSqlWithPk(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'id', 'data_type' => 'integer', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'NO', 'column_default' => null, 'udt_name' => 'int4']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([['column_name' => 'id']]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([]);
        $statementResults = [$statement, $statement2, $statement3];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $r = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"t\" (\n  \"id\" INTEGER NOT NULL,\n  PRIMARY KEY (\"id\")\n)", $r->getCreateStatement('t'));
    }
    public function testExactSqlWithCompositePk(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'a', 'data_type' => 'integer', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'NO', 'column_default' => null, 'udt_name' => 'int4'], ['column_name' => 'b', 'data_type' => 'integer', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'NO', 'column_default' => null, 'udt_name' => 'int4']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([['column_name' => 'a'], ['column_name' => 'b']]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([]);
        $statementResults = [$statement, $statement2, $statement3];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $r = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"t\" (\n  \"a\" INTEGER NOT NULL,\n  \"b\" INTEGER NOT NULL,\n  PRIMARY KEY (\"a\", \"b\")\n)", $r->getCreateStatement('t'));
    }
    public function testExactSqlWithUnique(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'e', 'data_type' => 'text', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'text']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([['constraint_name' => 'uq_e', 'column_name' => 'e']]);
        $statementResults = [$statement, $statement2, $statement3];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $r = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"t\" (\n  \"e\" TEXT,\n  CONSTRAINT \"uq_e\" UNIQUE (\"e\")\n)", $r->getCreateStatement('t'));
    }
    public function testExactSqlWithCompositeUnique(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'a', 'data_type' => 'integer', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'int4'], ['column_name' => 'b', 'data_type' => 'integer', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'int4']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([['constraint_name' => 'uq_ab', 'column_name' => 'a'], ['constraint_name' => 'uq_ab', 'column_name' => 'b']]);
        $statementResults = [$statement, $statement2, $statement3];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $r = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"t\" (\n  \"a\" INTEGER,\n  \"b\" INTEGER,\n  CONSTRAINT \"uq_ab\" UNIQUE (\"a\", \"b\")\n)", $r->getCreateStatement('t'));
    }
    public function testExactSqlWithPkAndUnique(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'id', 'data_type' => 'integer', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'NO', 'column_default' => null, 'udt_name' => 'int4'], ['column_name' => 'email', 'data_type' => 'text', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'NO', 'column_default' => null, 'udt_name' => 'text']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([['column_name' => 'id']]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([['constraint_name' => 'uq_email', 'column_name' => 'email']]);
        $statementResults = [$statement, $statement2, $statement3];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $r = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"t\" (\n  \"id\" INTEGER NOT NULL,\n  \"email\" TEXT NOT NULL,\n  PRIMARY KEY (\"id\"),\n  CONSTRAINT \"uq_email\" UNIQUE (\"email\")\n)", $r->getCreateStatement('t'));
    }
    public function testPartialUniqueIndexesReflectsPartialUniqueIndexesWithoutFlatteningTheirPredicate(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'email', 'data_type' => 'text', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'text'], ['column_name' => 'status', 'data_type' => 'text', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'text']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([['constraint_name' => 'users_active_email', 'column_name' => 'email', 'predicate' => "status = 'active'::text"], ['constraint_name' => 'users_expression', 'column_name' => null, 'predicate' => 'lower(email) IS NOT NULL']]);
        $statementResults = [$statement, $statement2, $statement3];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $reflector = new PgSqlSchemaReflector($connection);
        $createSql = $reflector->getCreateStatement('users');
        $indexes = $reflector->partialUniqueIndexes();
        self::assertSame("CREATE TABLE \"users\" (\n  \"email\" TEXT,\n  \"status\" TEXT\n)", $createSql);
        self::assertSame(['users_active_email'], array_keys($indexes['users']));
        self::assertSame(['email'], $indexes['users']['users_active_email']->columns);
        self::assertSame("status = 'active'::text", $indexes['users']['users_active_email']->predicate);
    }
    public function testCollectsCompositePartialIndexesAndDiscardsMalformedMetadata(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'email', 'data_type' => 'text', 'is_nullable' => 'YES', 'udt_name' => 'text'], ['column_name' => 'tenant_id', 'data_type' => 'integer', 'is_nullable' => 'NO', 'udt_name' => 'int4'], ['column_name' => 'status', 'data_type' => 'text', 'is_nullable' => 'YES', 'udt_name' => 'text']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([['constraint_name' => null, 'column_name' => null, 'predicate' => null], ['constraint_name' => '', 'column_name' => 'email', 'predicate' => null], ['constraint_name' => 'users_active_email', 'column_name' => 'email', 'predicate' => "status = 'active'"], ['constraint_name' => 'users_active_email', 'column_name' => 'tenant_id', 'predicate' => "status = 'active'"], ['constraint_name' => 'users_broken', 'column_name' => null, 'predicate' => 'lower(email) IS NOT NULL'], ['constraint_name' => 'users_broken', 'column_name' => 'email', 'predicate' => 'lower(email) IS NOT NULL'], ['constraint_name' => 'users_status', 'column_name' => 'status', 'predicate' => '   '], ['constraint_name' => 'users_pending_email', 'column_name' => 'email', 'predicate' => "status = 'pending'"]]);
        $statement4 = self::createStub(StatementInterface::class);
        $statement4->method('fetchAll')->willReturn([]);
        $statementResults = [$statement, $statement2, $statement3, $statement4];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $reflector = new PgSqlSchemaReflector($connection);
        $createSql = $reflector->getCreateStatement('users');
        $indexes = $reflector->partialUniqueIndexes()['users'];
        self::assertNotNull($createSql);
        self::assertStringContainsString('CONSTRAINT "users_status" UNIQUE ("status")', $createSql);
        self::assertStringNotContainsString('users_broken', $createSql);
        self::assertSame(['users_active_email', 'users_pending_email'], array_keys($indexes));
        self::assertSame(['email', 'tenant_id'], $indexes['users_active_email']->columns);
        self::assertSame("status = 'active'", $indexes['users_active_email']->predicate);
    }
    public function testEscapesTableNameInEveryMetadataQuery(): void
    {
        $queries = [];
        $columns = self::createStub(StatementInterface::class);
        $columns->method('fetchAll')->willReturn([['column_name' => 'id', 'data_type' => 'integer', 'is_nullable' => 'NO', 'udt_name' => 'int4']]);
        $empty = self::createStub(StatementInterface::class);
        $empty->method('fetchAll')->willReturn([]);
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(function (string $sql) use (&$queries, $columns, $empty): StatementInterface {
            $queries[] = $sql;
            return str_contains($sql, 'information_schema.columns') ? $columns : $empty;
        });
        self::assertNotNull((new PgSqlSchemaReflector($connection))->getCreateStatement("it's"));
        self::assertCount(4, $queries);
        self::assertStringContainsString("'it''s'", $queries[0]);
        self::assertStringContainsString("'it''s'", $queries[1]);
        self::assertStringContainsString("'it''s'", $queries[2]);
        self::assertStringContainsString("'it''s'", $queries[3]);
    }
    public function testVerifyColumnsQueryExact(): void
    {
        $queries = [];
        $colStmt = static::createStub(StatementInterface::class);
        $colStmt->method('fetchAll')->willReturn([]);
        $connection = static::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(function (string $q) use (&$queries, $colStmt) {
            $queries[] = $q;
            return $colStmt;
        });
        $reflector = new PgSqlSchemaReflector($connection);
        $reflector->getCreateStatement('users');
        self::assertSame('SELECT column_name, data_type, character_maximum_length, ' . 'numeric_precision, numeric_scale, is_nullable, column_default, ' . 'udt_name, domain_schema, domain_name, is_identity, identity_generation, ' . 'is_generated, generation_expression ' . 'FROM information_schema.columns ' . "WHERE table_schema = current_schema() AND table_name = 'users' " . 'ORDER BY ordinal_position', $queries[0]);
    }
    public function testReflectsSchemaQualifiedDomainTypeInsteadOfItsBaseType(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'amount', 'data_type' => 'numeric', 'character_maximum_length' => null, 'numeric_precision' => 5, 'numeric_scale' => 2, 'is_nullable' => 'NO', 'column_default' => null, 'udt_name' => 'numeric', 'domain_schema' => 'tenant "one"', 'domain_name' => 'Percentage']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([]);
        $statementResults = [$statement, $statement2, $statement3];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $reflector = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"payments\" (\n  \"amount\" \"tenant \"\"one\"\"\".\"Percentage\" NOT NULL\n)", $reflector->getCreateStatement('payments'));
    }
    public function testReflectsDomainWithoutSchemaAsAQuotedType(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'age', 'data_type' => 'integer', 'is_nullable' => 'YES', 'udt_name' => 'int4', 'domain_schema' => null, 'domain_name' => 'PositiveValue']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([]);
        $statementResults = [$statement, $statement2, $statement3];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $reflector = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"contacts\" (\n  \"age\" \"PositiveValue\"\n)", $reflector->getCreateStatement('contacts'));
    }
    public function testReflectsGeneratedAndIdentityColumns(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'total', 'data_type' => 'numeric', 'character_maximum_length' => null, 'numeric_precision' => 10, 'numeric_scale' => 2, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'numeric', 'is_identity' => 'NO', 'identity_generation' => null, 'is_generated' => 'ALWAYS', 'generation_expression' => 'qty * unit_price'], ['column_name' => 'id', 'data_type' => 'bigint', 'character_maximum_length' => null, 'numeric_precision' => 64, 'numeric_scale' => 0, 'is_nullable' => 'NO', 'column_default' => null, 'udt_name' => 'int8', 'is_identity' => 'YES', 'identity_generation' => 'ALWAYS', 'is_generated' => 'NEVER', 'generation_expression' => null], ['column_name' => 'ordinary', 'data_type' => 'integer', 'character_maximum_length' => null, 'numeric_precision' => 32, 'numeric_scale' => 0, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'int4', 'is_identity' => 'NO', 'identity_generation' => null, 'is_generated' => 'NEVER', 'generation_expression' => 'qty + 1'], ['column_name' => 'blank', 'data_type' => 'integer', 'character_maximum_length' => null, 'numeric_precision' => 32, 'numeric_scale' => 0, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'int4', 'is_identity' => 'NO', 'identity_generation' => null, 'is_generated' => 'ALWAYS', 'generation_expression' => '   ']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([]);
        $statementResults = [$statement, $statement2, $statement3];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $reflector = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"orders\" (\n  \"total\" NUMERIC(10,2) GENERATED ALWAYS AS (qty * unit_price) STORED,\n  \"id\" BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY,\n  \"ordinary\" INTEGER,\n  \"blank\" INTEGER\n)", $reflector->getCreateStatement('orders'));
    }
    public function testVerifyPkQueryExact(): void
    {
        $queries = [];
        $colStmt = static::createStub(StatementInterface::class);
        $colStmt->method('fetchAll')->willReturn([['column_name' => 'id', 'data_type' => 'integer', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'int4']]);
        $pkStmt = static::createStub(StatementInterface::class);
        $pkStmt->method('fetchAll')->willReturn([]);
        $uniqueStmt = static::createStub(StatementInterface::class);
        $uniqueStmt->method('fetchAll')->willReturn([]);
        $connection = static::createStub(ConnectionInterface::class);
        $callCount = 0;
        $connection->method('query')->willReturnCallback(function (string $q) use (&$callCount, &$queries, $colStmt, $pkStmt, $uniqueStmt) {
            $callCount++;
            $queries[] = $q;
            return match ($callCount) {
                1 => $colStmt,
                2 => $pkStmt,
                3 => $uniqueStmt,
                default => false,
            };
        });
        $reflector = new PgSqlSchemaReflector($connection);
        $reflector->getCreateStatement('my_table');
        self::assertSame('SELECT kcu.column_name ' . 'FROM information_schema.table_constraints tc ' . 'JOIN information_schema.key_column_usage kcu ' . '  ON tc.constraint_name = kcu.constraint_name ' . '  AND tc.table_schema = kcu.table_schema ' . 'WHERE tc.table_schema = current_schema() ' . "  AND tc.table_name = 'my_table' " . "  AND tc.constraint_type = 'PRIMARY KEY' " . 'ORDER BY kcu.ordinal_position', $queries[1]);
        self::assertSame('SELECT index_relation.relname AS constraint_name, attribute.attname AS column_name, ' . 'pg_get_expr(index_metadata.indpred, index_metadata.indrelid) AS predicate ' . 'FROM pg_catalog.pg_class table_relation ' . 'JOIN pg_catalog.pg_namespace namespace ON namespace.oid = table_relation.relnamespace ' . 'JOIN pg_catalog.pg_index index_metadata ON index_metadata.indrelid = table_relation.oid ' . 'JOIN pg_catalog.pg_class index_relation ON index_relation.oid = index_metadata.indexrelid ' . 'JOIN LATERAL unnest(index_metadata.indkey) WITH ORDINALITY key_column(attnum, ordinality) ' . '  ON key_column.ordinality <= index_metadata.indnkeyatts ' . 'LEFT JOIN pg_catalog.pg_attribute attribute ' . '  ON attribute.attrelid = table_relation.oid AND attribute.attnum = key_column.attnum ' . 'WHERE namespace.nspname = current_schema() ' . "  AND table_relation.relname = 'my_table' " . '  AND index_metadata.indisunique ' . '  AND index_metadata.indisvalid ' . '  AND NOT index_metadata.indisprimary ' . 'ORDER BY index_relation.relname, key_column.ordinality', $queries[2]);
    }
    public function testVerifyReflectAllListQuery(): void
    {
        $queries = [];
        $tableStmt = static::createStub(StatementInterface::class);
        $tableStmt->method('fetchAll')->willReturn([]);
        $connection = static::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(function (string $q) use (&$queries, $tableStmt) {
            $queries[] = $q;
            return $tableStmt;
        });
        $reflector = new PgSqlSchemaReflector($connection);
        $reflector->reflectAll();
        self::assertSame('SELECT table_name FROM information_schema.tables ' . "WHERE table_schema = current_schema() AND table_type = 'BASE TABLE' " . 'ORDER BY table_name', $queries[0]);
    }
    public function testReflectAllExactResults(): void
    {
        $tableStmt = static::createStub(StatementInterface::class);
        $tableStmt->method('fetchAll')->willReturn([['table_name' => 'items']]);
        $colStmt = static::createStub(StatementInterface::class);
        $colStmt->method('fetchAll')->willReturn([['column_name' => 'id', 'data_type' => 'integer', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'NO', 'column_default' => null, 'udt_name' => 'int4']]);
        $emptyStmt = static::createStub(StatementInterface::class);
        $emptyStmt->method('fetchAll')->willReturn([]);
        $connection = static::createStub(ConnectionInterface::class);
        $callCount = 0;
        $connection->method('query')->willReturnCallback(function () use (&$callCount, $tableStmt, $colStmt, $emptyStmt) {
            $callCount++;
            return match ($callCount) {
                1 => $tableStmt,
                2 => $colStmt,
                3 => $emptyStmt,
                4 => $emptyStmt,
                default => false,
            };
        });
        $reflector = new PgSqlSchemaReflector($connection);
        $result = $reflector->reflectAll();
        self::assertSame(['items' => "CREATE TABLE \"items\" (\n  \"id\" INTEGER NOT NULL\n)"], $result);
    }
    public function testReflectAllSkipsInvalidNames(): void
    {
        $tableStmt = static::createStub(StatementInterface::class);
        $tableStmt->method('fetchAll')->willReturn([['table_name' => ''], ['table_name' => null], ['other' => 'x']]);
        $connection = static::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturn($tableStmt);
        $reflector = new PgSqlSchemaReflector($connection);
        self::assertSame([], $reflector->reflectAll());
    }
    public function testReflectAllReturnsEmptyOnFailure(): void
    {
        $connection = static::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturn(false);
        $reflector = new PgSqlSchemaReflector($connection);
        self::assertSame([], $reflector->reflectAll());
    }
    public function testPkQueryReturnsFalseStillWorks(): void
    {
        $colStmt = static::createStub(StatementInterface::class);
        $colStmt->method('fetchAll')->willReturn([['column_name' => 'id', 'data_type' => 'integer', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'int4']]);
        $uniqueStmt = static::createStub(StatementInterface::class);
        $uniqueStmt->method('fetchAll')->willReturn([]);
        $connection = static::createStub(ConnectionInterface::class);
        $callCount = 0;
        $connection->method('query')->willReturnCallback(function () use (&$callCount, $colStmt, $uniqueStmt) {
            $callCount++;
            return match ($callCount) {
                1 => $colStmt,
                2 => false,
                3 => $uniqueStmt,
                default => false,
            };
        });
        $reflector = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"t\" (\n  \"id\" INTEGER\n)", $reflector->getCreateStatement('t'));
    }
    public function testUniqueQueryReturnsFalseStillWorks(): void
    {
        $colStmt = static::createStub(StatementInterface::class);
        $colStmt->method('fetchAll')->willReturn([['column_name' => 'id', 'data_type' => 'integer', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'int4']]);
        $pkStmt = static::createStub(StatementInterface::class);
        $pkStmt->method('fetchAll')->willReturn([]);
        $connection = static::createStub(ConnectionInterface::class);
        $callCount = 0;
        $connection->method('query')->willReturnCallback(function () use (&$callCount, $colStmt, $pkStmt) {
            $callCount++;
            return match ($callCount) {
                1 => $colStmt,
                2 => $pkStmt,
                3 => false,
                default => false,
            };
        });
        $reflector = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"t\" (\n  \"id\" INTEGER\n)", $reflector->getCreateStatement('t'));
    }
    public function testTableNameEscaping(): void
    {
        $queries = [];
        $colStmt = static::createStub(StatementInterface::class);
        $colStmt->method('fetchAll')->willReturn([]);
        $connection = static::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(function (string $q) use (&$queries, $colStmt) {
            $queries[] = $q;
            return $colStmt;
        });
        $reflector = new PgSqlSchemaReflector($connection);
        $reflector->getCreateStatement("it's");
        self::assertStringContainsString("table_name = 'it''s'", $queries[0]);
    }
    public function testNumericStringPrecisionScale(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'x', 'data_type' => 'numeric', 'character_maximum_length' => null, 'numeric_precision' => '8', 'numeric_scale' => '3', 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'numeric']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([]);
        $statementResults = [$statement, $statement2, $statement3];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $r = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" NUMERIC(8,3)\n)", $r->getCreateStatement('t'));
    }
    public function testVarcharStringLength(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'x', 'data_type' => 'character varying', 'character_maximum_length' => '100', 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'varchar']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([]);
        $statementResults = [$statement, $statement2, $statement3];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $r = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" VARCHAR(100)\n)", $r->getCreateStatement('t'));
    }
    public function testCharStringLength(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'x', 'data_type' => 'character', 'character_maximum_length' => '3', 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'bpchar']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([]);
        $statementResults = [$statement, $statement2, $statement3];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $r = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" CHAR(3)\n)", $r->getCreateStatement('t'));
    }
    public function testNumericStringPrecisionOnly(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'x', 'data_type' => 'numeric', 'character_maximum_length' => null, 'numeric_precision' => '12', 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'numeric']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([]);
        $statementResults = [$statement, $statement2, $statement3];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $r = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" NUMERIC(12)\n)", $r->getCreateStatement('t'));
    }
    public function testMissingColumnNameDefaults(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['data_type' => 'integer', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'int4']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([]);
        $statementResults = [$statement, $statement2, $statement3];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $r = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"t\" (\n  \"\" INTEGER\n)", $r->getCreateStatement('t'));
    }
    public function testMissingDataTypeDefaultsToText(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'x', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => '']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([]);
        $statementResults = [$statement, $statement2, $statement3];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $r = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" TEXT\n)", $r->getCreateStatement('t'));
    }
    public function testUnknownDataTypePassedThrough(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'x', 'data_type' => 'xml', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'xml']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([]);
        $statementResults = [$statement, $statement2, $statement3];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $r = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" XML\n)", $r->getCreateStatement('t'));
    }
    public function testReflectAllMultipleTablesExactResults(): void
    {
        $tableStmt = static::createStub(StatementInterface::class);
        $tableStmt->method('fetchAll')->willReturn([['table_name' => 'alpha'], ['table_name' => 'beta']]);
        $colStmtA = static::createStub(StatementInterface::class);
        $colStmtA->method('fetchAll')->willReturn([['column_name' => 'id', 'data_type' => 'integer', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'NO', 'column_default' => null, 'udt_name' => 'int4']]);
        $colStmtB = static::createStub(StatementInterface::class);
        $colStmtB->method('fetchAll')->willReturn([['column_name' => 'val', 'data_type' => 'text', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'text']]);
        $emptyStmt = static::createStub(StatementInterface::class);
        $emptyStmt->method('fetchAll')->willReturn([]);
        $connection = static::createStub(ConnectionInterface::class);
        $callCount = 0;
        $connection->method('query')->willReturnCallback(function () use (&$callCount, $tableStmt, $colStmtA, $colStmtB, $emptyStmt) {
            $callCount++;
            return match ($callCount) {
                1 => $tableStmt,
                2 => $colStmtA,
                3 => $emptyStmt,
                4 => $emptyStmt,
                5 => $emptyStmt,
                6 => $colStmtB,
                7 => $emptyStmt,
                8 => $emptyStmt,
                9 => $emptyStmt,
                default => false,
            };
        });
        $reflector = new PgSqlSchemaReflector($connection);
        $result = $reflector->reflectAll();
        self::assertCount(2, $result);
        self::assertArrayHasKey('alpha', $result);
        self::assertArrayHasKey('beta', $result);
        self::assertSame("CREATE TABLE \"alpha\" (\n  \"id\" INTEGER NOT NULL\n)", $result['alpha']);
        self::assertSame("CREATE TABLE \"beta\" (\n  \"val\" TEXT\n)", $result['beta']);
    }
    public function testReflectAllSkipsTablesWithNoColumns(): void
    {
        $tableStmt = static::createStub(StatementInterface::class);
        $tableStmt->method('fetchAll')->willReturn([['table_name' => 'good'], ['table_name' => 'empty_cols']]);
        $colStmtGood = static::createStub(StatementInterface::class);
        $colStmtGood->method('fetchAll')->willReturn([['column_name' => 'id', 'data_type' => 'integer', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'int4']]);
        $colStmtEmpty = static::createStub(StatementInterface::class);
        $colStmtEmpty->method('fetchAll')->willReturn([]);
        $emptyStmt = static::createStub(StatementInterface::class);
        $emptyStmt->method('fetchAll')->willReturn([]);
        $connection = static::createStub(ConnectionInterface::class);
        $callCount = 0;
        $connection->method('query')->willReturnCallback(function () use (&$callCount, $tableStmt, $colStmtGood, $colStmtEmpty, $emptyStmt) {
            $callCount++;
            return match ($callCount) {
                1 => $tableStmt,
                2 => $colStmtGood,
                3 => $emptyStmt,
                4 => $emptyStmt,
                5 => $emptyStmt,
                6 => $colStmtEmpty,
                default => false,
            };
        });
        $reflector = new PgSqlSchemaReflector($connection);
        $result = $reflector->reflectAll();
        self::assertCount(1, $result);
        self::assertArrayHasKey('good', $result);
    }
    public function testNumericNonIntPrecisionCastsToInt(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'x', 'data_type' => 'numeric', 'character_maximum_length' => null, 'numeric_precision' => 'abc', 'numeric_scale' => 'xyz', 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'numeric']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([]);
        $statementResults = [$statement, $statement2, $statement3];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $r = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" NUMERIC(0,0)\n)", $r->getCreateStatement('t'));
    }
    public function testNumericIntPrecisionAndScaleExactOutput(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'x', 'data_type' => 'numeric', 'character_maximum_length' => null, 'numeric_precision' => 5, 'numeric_scale' => 3, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'numeric']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([]);
        $statementResults = [$statement, $statement2, $statement3];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $r = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" NUMERIC(5,3)\n)", $r->getCreateStatement('t'));
    }
    public function testNumericPrecisionOnlyNonIntCastsToInt(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'x', 'data_type' => 'numeric', 'character_maximum_length' => null, 'numeric_precision' => 'bad', 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'numeric']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([]);
        $statementResults = [$statement, $statement2, $statement3];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $r = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" NUMERIC(0)\n)", $r->getCreateStatement('t'));
    }
    public function testArrayWithLowercaseUdtName(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'x', 'data_type' => 'ARRAY', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => '_text']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([]);
        $statementResults = [$statement, $statement2, $statement3];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $r = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" TEXT[]\n)", $r->getCreateStatement('t'));
    }
    public function testTableNameWithSingleQuoteInReflectAll(): void
    {
        $tableStmt = static::createStub(StatementInterface::class);
        $tableStmt->method('fetchAll')->willReturn([['table_name' => "it's"]]);
        $colStmt = static::createStub(StatementInterface::class);
        $colStmt->method('fetchAll')->willReturn([['column_name' => 'id', 'data_type' => 'integer', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'int4']]);
        $emptyStmt = static::createStub(StatementInterface::class);
        $emptyStmt->method('fetchAll')->willReturn([]);
        $connection = static::createStub(ConnectionInterface::class);
        $callCount = 0;
        $connection->method('query')->willReturnCallback(function (string $q) use (&$callCount, $tableStmt, $colStmt, $emptyStmt) {
            $callCount++;
            return match ($callCount) {
                1 => $tableStmt,
                2 => $colStmt,
                3 => $emptyStmt,
                4 => $emptyStmt,
                default => false,
            };
        });
        $reflector = new PgSqlSchemaReflector($connection);
        $result = $reflector->reflectAll();
        self::assertCount(1, $result);
        self::assertArrayHasKey("it's", $result);
    }
    public function testUniqueConstraintNonStringColumnsAreSkipped(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'id', 'data_type' => 'integer', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'int4']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([['constraint_name' => null, 'column_name' => null]]);
        $statementResults = [$statement, $statement2, $statement3];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $r = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"t\" (\n  \"id\" INTEGER\n)", $r->getCreateStatement('t'));
    }
    public function testPkNonStringColumnNameIsSkipped(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'id', 'data_type' => 'integer', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'int4']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([['column_name' => null]]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([]);
        $statementResults = [$statement, $statement2, $statement3];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $r = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"t\" (\n  \"id\" INTEGER\n)", $r->getCreateStatement('t'));
    }
    public function testForeignKeysAreReconstructedWithCompositeColumnsAndActions(): void
    {
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'tenant_id', 'data_type' => 'integer', 'is_nullable' => 'NO', 'udt_name' => 'int4'], ['column_name' => 'parent_id', 'data_type' => 'integer', 'is_nullable' => 'NO', 'udt_name' => 'int4']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([]);
        $statement4 = self::createStub(StatementInterface::class);
        $statement4->method('fetchAll')->willReturn([['constraint_name' => 'fk_parent', 'column_name' => 'tenant_id', 'foreign_table_name' => 'parents', 'foreign_column_name' => 'tenant_id', 'update_rule' => 'CASCADE', 'delete_rule' => 'CASCADE'], ['constraint_name' => 'fk_parent', 'column_name' => 'parent_id', 'foreign_table_name' => 'parents', 'foreign_column_name' => 'id', 'update_rule' => 'CASCADE', 'delete_rule' => 'CASCADE']]);
        $statementResults = [$statement, $statement2, $statement3, $statement4];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $r = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"children\" (\n" . "  \"tenant_id\" INTEGER NOT NULL,\n" . "  \"parent_id\" INTEGER NOT NULL,\n" . '  CONSTRAINT "fk_parent" FOREIGN KEY ("tenant_id", "parent_id") ' . "REFERENCES \"parents\" (\"tenant_id\", \"id\") ON UPDATE CASCADE ON DELETE CASCADE\n)", $r->getCreateStatement('children'));
    }
    public function testForeignKeyQueryIsExactAndEscapesTableName(): void
    {
        $queries = [];
        $columnStatement = static::createStub(StatementInterface::class);
        $columnStatement->method('fetchAll')->willReturn([['column_name' => 'id', 'data_type' => 'integer', 'is_nullable' => 'NO', 'udt_name' => 'int4']]);
        $emptyStatement = static::createStub(StatementInterface::class);
        $emptyStatement->method('fetchAll')->willReturn([]);
        $connection = static::createStub(ConnectionInterface::class);
        $callCount = 0;
        $connection->method('query')->willReturnCallback(function (string $query) use (&$callCount, &$queries, $columnStatement, $emptyStatement) {
            $queries[] = $query;
            $callCount++;
            return $callCount === 1 ? $columnStatement : $emptyStatement;
        });
        (new PgSqlSchemaReflector($connection))->getCreateStatement("child'ren");
        self::assertSame('SELECT fk.constraint_name, fk.column_name, ' . 'pk.table_name AS foreign_table_name, pk.column_name AS foreign_column_name, ' . 'rc.update_rule, rc.delete_rule ' . 'FROM information_schema.referential_constraints rc ' . 'JOIN information_schema.key_column_usage fk ' . '  ON fk.constraint_catalog = rc.constraint_catalog ' . '  AND fk.constraint_schema = rc.constraint_schema ' . '  AND fk.constraint_name = rc.constraint_name ' . 'JOIN information_schema.key_column_usage pk ' . '  ON pk.constraint_catalog = rc.unique_constraint_catalog ' . '  AND pk.constraint_schema = rc.unique_constraint_schema ' . '  AND pk.constraint_name = rc.unique_constraint_name ' . '  AND pk.ordinal_position = fk.position_in_unique_constraint ' . 'WHERE fk.table_schema = current_schema() ' . "  AND fk.table_name = 'child''ren' " . 'ORDER BY fk.constraint_name, fk.ordinal_position', $queries[3]);
    }
    public function testMalformedForeignKeyRowsAreSkippedIndependently(): void
    {
        $valid = ['constraint_name' => 'fk_parent', 'column_name' => 'parent_id', 'foreign_table_name' => 'parents', 'foreign_column_name' => 'id', 'update_rule' => 'CASCADE', 'delete_rule' => 'CASCADE'];
        $rows = [array_replace($valid, ['constraint_name' => null]), array_replace($valid, ['column_name' => null]), array_replace($valid, ['foreign_table_name' => null]), array_replace($valid, ['foreign_column_name' => null]), array_replace($valid, ['update_rule' => null]), array_replace($valid, ['delete_rule' => null]), $valid];
        $statement = self::createStub(StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'parent_id', 'data_type' => 'integer', 'is_nullable' => 'NO', 'udt_name' => 'int4']]);
        $statement2 = self::createStub(StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([]);
        $statement3 = self::createStub(StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([]);
        $statement4 = self::createStub(StatementInterface::class);
        $statement4->method('fetchAll')->willReturn($rows);
        $statementResults = [$statement, $statement2, $statement3, $statement4];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $r = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"children\" (\n" . "  \"parent_id\" INTEGER NOT NULL,\n" . '  CONSTRAINT "fk_parent" FOREIGN KEY ("parent_id") ' . "REFERENCES \"parents\" (\"id\") ON UPDATE CASCADE ON DELETE CASCADE\n)", $r->getCreateStatement('children'));
    }
}
