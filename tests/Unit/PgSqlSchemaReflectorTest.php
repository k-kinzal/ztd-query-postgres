<?php

declare(strict_types=1);

namespace Tests\Unit;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Tests\Fake\FakeSequentialConnection;
use Tests\Fake\FakeStatement;
use ZtdQuery\Connection\ConnectionInterface;
use ZtdQuery\Connection\StatementInterface;
use ZtdQuery\Platform\Postgres\PgSqlSchemaReflector;

#[CoversClass(PgSqlSchemaReflector::class)]
final class PgSqlSchemaReflectorTest extends TestCase
{
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
        $r = new PgSqlSchemaReflector(new FakeSequentialConnection([
            new FakeStatement([
                ['column_name' => 'id', 'data_type' => 'integer', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'NO', 'column_default' => null, 'udt_name' => 'int4'],
            ]),
            new FakeStatement([]),
            new FakeStatement([]),
        ]));

        self::assertSame("CREATE TABLE \"t\" (\n  \"id\" INTEGER NOT NULL\n)", $r->getCreateStatement('t'));
    }

    public function testExactSqlForSmallint(): void
    {
        $r = new PgSqlSchemaReflector(new FakeSequentialConnection([
            new FakeStatement([
                ['column_name' => 'v', 'data_type' => 'smallint', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'int2'],
            ]),
            new FakeStatement([]),
            new FakeStatement([]),
        ]));

        self::assertSame("CREATE TABLE \"t\" (\n  \"v\" SMALLINT\n)", $r->getCreateStatement('t'));
    }

    public function testExactSqlForBigint(): void
    {
        $r = new PgSqlSchemaReflector(new FakeSequentialConnection([
            new FakeStatement([
                ['column_name' => 'b', 'data_type' => 'bigint', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'int8'],
            ]),
            new FakeStatement([]),
            new FakeStatement([]),
        ]));

        self::assertSame("CREATE TABLE \"t\" (\n  \"b\" BIGINT\n)", $r->getCreateStatement('t'));
    }

    public function testExactSqlForReal(): void
    {
        $r = new PgSqlSchemaReflector(new FakeSequentialConnection([
            new FakeStatement([
                ['column_name' => 'f', 'data_type' => 'real', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'float4'],
            ]),
            new FakeStatement([]),
            new FakeStatement([]),
        ]));

        self::assertSame("CREATE TABLE \"t\" (\n  \"f\" REAL\n)", $r->getCreateStatement('t'));
    }

    public function testExactSqlForDoublePrecision(): void
    {
        $r = new PgSqlSchemaReflector(new FakeSequentialConnection([
            new FakeStatement([
                ['column_name' => 'd', 'data_type' => 'double precision', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'float8'],
            ]),
            new FakeStatement([]),
            new FakeStatement([]),
        ]));

        self::assertSame("CREATE TABLE \"t\" (\n  \"d\" DOUBLE PRECISION\n)", $r->getCreateStatement('t'));
    }

    public function testExactSqlForBoolean(): void
    {
        $r = new PgSqlSchemaReflector(new FakeSequentialConnection([
            new FakeStatement([
                ['column_name' => 'a', 'data_type' => 'boolean', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'bool'],
            ]),
            new FakeStatement([]),
            new FakeStatement([]),
        ]));

        self::assertSame("CREATE TABLE \"t\" (\n  \"a\" BOOLEAN\n)", $r->getCreateStatement('t'));
    }

    public function testExactSqlForDate(): void
    {
        $r = new PgSqlSchemaReflector(new FakeSequentialConnection([
            new FakeStatement([
                ['column_name' => 'x', 'data_type' => 'date', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'date'],
            ]),
            new FakeStatement([]),
            new FakeStatement([]),
        ]));

        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" DATE\n)", $r->getCreateStatement('t'));
    }

    public function testExactSqlForTimestamp(): void
    {
        $r = new PgSqlSchemaReflector(new FakeSequentialConnection([
            new FakeStatement([
                ['column_name' => 'x', 'data_type' => 'timestamp without time zone', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'timestamp'],
            ]),
            new FakeStatement([]),
            new FakeStatement([]),
        ]));

        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" TIMESTAMP\n)", $r->getCreateStatement('t'));
    }

    public function testExactSqlForTimestamptz(): void
    {
        $r = new PgSqlSchemaReflector(new FakeSequentialConnection([
            new FakeStatement([
                ['column_name' => 'x', 'data_type' => 'timestamp with time zone', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'timestamptz'],
            ]),
            new FakeStatement([]),
            new FakeStatement([]),
        ]));

        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" TIMESTAMPTZ\n)", $r->getCreateStatement('t'));
    }

    public function testExactSqlForTime(): void
    {
        $r = new PgSqlSchemaReflector(new FakeSequentialConnection([
            new FakeStatement([
                ['column_name' => 'x', 'data_type' => 'time without time zone', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'time'],
            ]),
            new FakeStatement([]),
            new FakeStatement([]),
        ]));

        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" TIME\n)", $r->getCreateStatement('t'));
    }

    public function testExactSqlForTimetz(): void
    {
        $r = new PgSqlSchemaReflector(new FakeSequentialConnection([
            new FakeStatement([
                ['column_name' => 'x', 'data_type' => 'time with time zone', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'timetz'],
            ]),
            new FakeStatement([]),
            new FakeStatement([]),
        ]));

        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" TIMETZ\n)", $r->getCreateStatement('t'));
    }

    public function testExactSqlForText(): void
    {
        $r = new PgSqlSchemaReflector(new FakeSequentialConnection([
            new FakeStatement([
                ['column_name' => 'x', 'data_type' => 'text', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'text'],
            ]),
            new FakeStatement([]),
            new FakeStatement([]),
        ]));

        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" TEXT\n)", $r->getCreateStatement('t'));
    }

    public function testExactSqlForBytea(): void
    {
        $r = new PgSqlSchemaReflector(new FakeSequentialConnection([
            new FakeStatement([
                ['column_name' => 'x', 'data_type' => 'bytea', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'bytea'],
            ]),
            new FakeStatement([]),
            new FakeStatement([]),
        ]));

        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" BYTEA\n)", $r->getCreateStatement('t'));
    }

    public function testExactSqlForJson(): void
    {
        $r = new PgSqlSchemaReflector(new FakeSequentialConnection([
            new FakeStatement([
                ['column_name' => 'x', 'data_type' => 'json', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'json'],
            ]),
            new FakeStatement([]),
            new FakeStatement([]),
        ]));

        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" JSON\n)", $r->getCreateStatement('t'));
    }

    public function testExactSqlForJsonb(): void
    {
        $r = new PgSqlSchemaReflector(new FakeSequentialConnection([
            new FakeStatement([
                ['column_name' => 'x', 'data_type' => 'jsonb', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'jsonb'],
            ]),
            new FakeStatement([]),
            new FakeStatement([]),
        ]));

        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" JSONB\n)", $r->getCreateStatement('t'));
    }

    public function testExactSqlForUuid(): void
    {
        $r = new PgSqlSchemaReflector(new FakeSequentialConnection([
            new FakeStatement([
                ['column_name' => 'x', 'data_type' => 'uuid', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'uuid'],
            ]),
            new FakeStatement([]),
            new FakeStatement([]),
        ]));

        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" UUID\n)", $r->getCreateStatement('t'));
    }

    public function testExactSqlForVarcharWithLen(): void
    {
        $r = new PgSqlSchemaReflector(new FakeSequentialConnection([
            new FakeStatement([
                ['column_name' => 'x', 'data_type' => 'character varying', 'character_maximum_length' => 50, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'varchar'],
            ]),
            new FakeStatement([]),
            new FakeStatement([]),
        ]));

        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" VARCHAR(50)\n)", $r->getCreateStatement('t'));
    }

    public function testExactSqlForVarcharNoLen(): void
    {
        $r = new PgSqlSchemaReflector(new FakeSequentialConnection([
            new FakeStatement([
                ['column_name' => 'x', 'data_type' => 'character varying', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'varchar'],
            ]),
            new FakeStatement([]),
            new FakeStatement([]),
        ]));

        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" VARCHAR\n)", $r->getCreateStatement('t'));
    }

    public function testExactSqlForCharWithLen(): void
    {
        $r = new PgSqlSchemaReflector(new FakeSequentialConnection([
            new FakeStatement([
                ['column_name' => 'x', 'data_type' => 'character', 'character_maximum_length' => 5, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'bpchar'],
            ]),
            new FakeStatement([]),
            new FakeStatement([]),
        ]));

        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" CHAR(5)\n)", $r->getCreateStatement('t'));
    }

    public function testExactSqlForCharNoLen(): void
    {
        $r = new PgSqlSchemaReflector(new FakeSequentialConnection([
            new FakeStatement([
                ['column_name' => 'x', 'data_type' => 'character', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'bpchar'],
            ]),
            new FakeStatement([]),
            new FakeStatement([]),
        ]));

        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" CHAR(1)\n)", $r->getCreateStatement('t'));
    }

    public function testExactSqlForNumericPrecisionScale(): void
    {
        $r = new PgSqlSchemaReflector(new FakeSequentialConnection([
            new FakeStatement([
                ['column_name' => 'x', 'data_type' => 'numeric', 'character_maximum_length' => null, 'numeric_precision' => 10, 'numeric_scale' => 2, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'numeric'],
            ]),
            new FakeStatement([]),
            new FakeStatement([]),
        ]));

        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" NUMERIC(10,2)\n)", $r->getCreateStatement('t'));
    }

    public function testExactSqlForNumericPrecisionOnly(): void
    {
        $r = new PgSqlSchemaReflector(new FakeSequentialConnection([
            new FakeStatement([
                ['column_name' => 'x', 'data_type' => 'numeric', 'character_maximum_length' => null, 'numeric_precision' => 18, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'numeric'],
            ]),
            new FakeStatement([]),
            new FakeStatement([]),
        ]));

        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" NUMERIC(18)\n)", $r->getCreateStatement('t'));
    }

    public function testExactSqlForNumericBare(): void
    {
        $r = new PgSqlSchemaReflector(new FakeSequentialConnection([
            new FakeStatement([
                ['column_name' => 'x', 'data_type' => 'numeric', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'numeric'],
            ]),
            new FakeStatement([]),
            new FakeStatement([]),
        ]));

        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" NUMERIC\n)", $r->getCreateStatement('t'));
    }

    public function testExactSqlForUserDefinedCitext(): void
    {
        $r = new PgSqlSchemaReflector(new FakeSequentialConnection([
            new FakeStatement([
                ['column_name' => 'x', 'data_type' => 'USER-DEFINED', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'citext'],
            ]),
            new FakeStatement([]),
            new FakeStatement([]),
        ]));

        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" CITEXT\n)", $r->getCreateStatement('t'));
    }

    public function testExactSqlForUserDefinedHstore(): void
    {
        $r = new PgSqlSchemaReflector(new FakeSequentialConnection([
            new FakeStatement([
                ['column_name' => 'x', 'data_type' => 'USER-DEFINED', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'hstore'],
            ]),
            new FakeStatement([]),
            new FakeStatement([]),
        ]));

        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" HSTORE\n)", $r->getCreateStatement('t'));
    }

    public function testExactSqlForUserDefinedLtree(): void
    {
        $r = new PgSqlSchemaReflector(new FakeSequentialConnection([
            new FakeStatement([
                ['column_name' => 'x', 'data_type' => 'USER-DEFINED', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'ltree'],
            ]),
            new FakeStatement([]),
            new FakeStatement([]),
        ]));

        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" LTREE\n)", $r->getCreateStatement('t'));
    }

    public function testExactSqlForUserDefinedCustom(): void
    {
        $r = new PgSqlSchemaReflector(new FakeSequentialConnection([
            new FakeStatement([
                ['column_name' => 'x', 'data_type' => 'USER-DEFINED', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'myenum'],
            ]),
            new FakeStatement([]),
            new FakeStatement([]),
        ]));

        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" MYENUM\n)", $r->getCreateStatement('t'));
    }

    public function testExactSqlForUserDefinedEmpty(): void
    {
        $r = new PgSqlSchemaReflector(new FakeSequentialConnection([
            new FakeStatement([
                ['column_name' => 'x', 'data_type' => 'USER-DEFINED', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => ''],
            ]),
            new FakeStatement([]),
            new FakeStatement([]),
        ]));

        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" TEXT\n)", $r->getCreateStatement('t'));
    }

    public function testExactSqlForArrayWithUnderscore(): void
    {
        $r = new PgSqlSchemaReflector(new FakeSequentialConnection([
            new FakeStatement([
                ['column_name' => 'x', 'data_type' => 'ARRAY', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => '_int4'],
            ]),
            new FakeStatement([]),
            new FakeStatement([]),
        ]));

        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" INT4[]\n)", $r->getCreateStatement('t'));
    }

    public function testExactSqlForArrayNoUnderscore(): void
    {
        $r = new PgSqlSchemaReflector(new FakeSequentialConnection([
            new FakeStatement([
                ['column_name' => 'x', 'data_type' => 'ARRAY', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'mytypes'],
            ]),
            new FakeStatement([]),
            new FakeStatement([]),
        ]));

        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" MYTYPES[]\n)", $r->getCreateStatement('t'));
    }

    public function testExactSqlWithDefault(): void
    {
        $r = new PgSqlSchemaReflector(new FakeSequentialConnection([
            new FakeStatement([
                ['column_name' => 's', 'data_type' => 'text', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'NO', 'column_default' => "'active'::text", 'udt_name' => 'text'],
            ]),
            new FakeStatement([]),
            new FakeStatement([]),
        ]));

        self::assertSame("CREATE TABLE \"t\" (\n  \"s\" TEXT NOT NULL DEFAULT 'active'::text\n)", $r->getCreateStatement('t'));
    }

    public function testExactSqlWithPk(): void
    {
        $r = new PgSqlSchemaReflector(new FakeSequentialConnection([
            new FakeStatement([
                ['column_name' => 'id', 'data_type' => 'integer', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'NO', 'column_default' => null, 'udt_name' => 'int4'],
            ]),
            new FakeStatement([
                ['column_name' => 'id'],
            ]),
            new FakeStatement([]),
        ]));

        self::assertSame("CREATE TABLE \"t\" (\n  \"id\" INTEGER NOT NULL,\n  PRIMARY KEY (\"id\")\n)", $r->getCreateStatement('t'));
    }

    public function testExactSqlWithCompositePk(): void
    {
        $r = new PgSqlSchemaReflector(new FakeSequentialConnection([
            new FakeStatement([
                ['column_name' => 'a', 'data_type' => 'integer', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'NO', 'column_default' => null, 'udt_name' => 'int4'],
                ['column_name' => 'b', 'data_type' => 'integer', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'NO', 'column_default' => null, 'udt_name' => 'int4'],
            ]),
            new FakeStatement([
                ['column_name' => 'a'],
                ['column_name' => 'b'],
            ]),
            new FakeStatement([]),
        ]));

        self::assertSame(
            "CREATE TABLE \"t\" (\n  \"a\" INTEGER NOT NULL,\n  \"b\" INTEGER NOT NULL,\n  PRIMARY KEY (\"a\", \"b\")\n)",
            $r->getCreateStatement('t')
        );
    }

    public function testExactSqlWithUnique(): void
    {
        $r = new PgSqlSchemaReflector(new FakeSequentialConnection([
            new FakeStatement([
                ['column_name' => 'e', 'data_type' => 'text', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'text'],
            ]),
            new FakeStatement([]),
            new FakeStatement([
                ['constraint_name' => 'uq_e', 'column_name' => 'e'],
            ]),
        ]));

        self::assertSame(
            "CREATE TABLE \"t\" (\n  \"e\" TEXT,\n  CONSTRAINT \"uq_e\" UNIQUE (\"e\")\n)",
            $r->getCreateStatement('t')
        );
    }

    public function testExactSqlWithCompositeUnique(): void
    {
        $r = new PgSqlSchemaReflector(new FakeSequentialConnection([
            new FakeStatement([
                ['column_name' => 'a', 'data_type' => 'integer', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'int4'],
                ['column_name' => 'b', 'data_type' => 'integer', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'int4'],
            ]),
            new FakeStatement([]),
            new FakeStatement([
                ['constraint_name' => 'uq_ab', 'column_name' => 'a'],
                ['constraint_name' => 'uq_ab', 'column_name' => 'b'],
            ]),
        ]));

        self::assertSame(
            "CREATE TABLE \"t\" (\n  \"a\" INTEGER,\n  \"b\" INTEGER,\n  CONSTRAINT \"uq_ab\" UNIQUE (\"a\", \"b\")\n)",
            $r->getCreateStatement('t')
        );
    }

    public function testExactSqlWithPkAndUnique(): void
    {
        $r = new PgSqlSchemaReflector(new FakeSequentialConnection([
            new FakeStatement([
                ['column_name' => 'id', 'data_type' => 'integer', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'NO', 'column_default' => null, 'udt_name' => 'int4'],
                ['column_name' => 'email', 'data_type' => 'text', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'NO', 'column_default' => null, 'udt_name' => 'text'],
            ]),
            new FakeStatement([
                ['column_name' => 'id'],
            ]),
            new FakeStatement([
                ['constraint_name' => 'uq_email', 'column_name' => 'email'],
            ]),
        ]));

        self::assertSame(
            "CREATE TABLE \"t\" (\n  \"id\" INTEGER NOT NULL,\n  \"email\" TEXT NOT NULL,\n  PRIMARY KEY (\"id\"),\n  CONSTRAINT \"uq_email\" UNIQUE (\"email\")\n)",
            $r->getCreateStatement('t')
        );
    }

    public function testVerifyColumnsQueryExact(): void
    {
        $queries = [];

        $colStmt = static::createStub(StatementInterface::class);
        $colStmt->method('fetchAll')->willReturn([]);

        $connection = static::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(
            function (string $q) use (&$queries, $colStmt) {
                $queries[] = $q;

                return $colStmt;
            }
        );

        $reflector = new PgSqlSchemaReflector($connection);
        $reflector->getCreateStatement('users');

        self::assertSame(
            "SELECT column_name, data_type, character_maximum_length, "
            . "numeric_precision, numeric_scale, is_nullable, column_default, "
            . "udt_name "
            . "FROM information_schema.columns "
            . "WHERE table_schema = current_schema() AND table_name = 'users' "
            . "ORDER BY ordinal_position",
            $queries[0]
        );
    }

    public function testVerifyPkQueryExact(): void
    {
        $queries = [];

        $colStmt = static::createStub(StatementInterface::class);
        $colStmt->method('fetchAll')->willReturn([
            ['column_name' => 'id', 'data_type' => 'integer', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'int4'],
        ]);

        $pkStmt = static::createStub(StatementInterface::class);
        $pkStmt->method('fetchAll')->willReturn([]);

        $uniqueStmt = static::createStub(StatementInterface::class);
        $uniqueStmt->method('fetchAll')->willReturn([]);

        $connection = static::createStub(ConnectionInterface::class);
        $callCount = 0;
        $connection->method('query')->willReturnCallback(
            function (string $q) use (&$callCount, &$queries, $colStmt, $pkStmt, $uniqueStmt) {
                $callCount++;
                $queries[] = $q;

                return match ($callCount) {
                    1 => $colStmt,
                    2 => $pkStmt,
                    3 => $uniqueStmt,
                    default => false,
                };
            }
        );

        $reflector = new PgSqlSchemaReflector($connection);
        $reflector->getCreateStatement('my_table');

        self::assertSame(
            "SELECT kcu.column_name "
            . "FROM information_schema.table_constraints tc "
            . "JOIN information_schema.key_column_usage kcu "
            . "  ON tc.constraint_name = kcu.constraint_name "
            . "  AND tc.table_schema = kcu.table_schema "
            . "WHERE tc.table_schema = current_schema() "
            . "  AND tc.table_name = 'my_table' "
            . "  AND tc.constraint_type = 'PRIMARY KEY' "
            . "ORDER BY kcu.ordinal_position",
            $queries[1]
        );

        self::assertSame(
            "SELECT tc.constraint_name, kcu.column_name "
            . "FROM information_schema.table_constraints tc "
            . "JOIN information_schema.key_column_usage kcu "
            . "  ON tc.constraint_name = kcu.constraint_name "
            . "  AND tc.table_schema = kcu.table_schema "
            . "WHERE tc.table_schema = current_schema() "
            . "  AND tc.table_name = 'my_table' "
            . "  AND tc.constraint_type = 'UNIQUE' "
            . "ORDER BY tc.constraint_name, kcu.ordinal_position",
            $queries[2]
        );
    }

    public function testVerifyReflectAllListQuery(): void
    {
        $queries = [];

        $tableStmt = static::createStub(StatementInterface::class);
        $tableStmt->method('fetchAll')->willReturn([]);

        $connection = static::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(
            function (string $q) use (&$queries, $tableStmt) {
                $queries[] = $q;

                return $tableStmt;
            }
        );

        $reflector = new PgSqlSchemaReflector($connection);
        $reflector->reflectAll();

        self::assertSame(
            "SELECT table_name FROM information_schema.tables "
            . "WHERE table_schema = current_schema() AND table_type = 'BASE TABLE' "
            . "ORDER BY table_name",
            $queries[0]
        );
    }

    public function testReflectAllExactResults(): void
    {
        $tableStmt = static::createStub(StatementInterface::class);
        $tableStmt->method('fetchAll')->willReturn([
            ['table_name' => 'items'],
        ]);

        $colStmt = static::createStub(StatementInterface::class);
        $colStmt->method('fetchAll')->willReturn([
            ['column_name' => 'id', 'data_type' => 'integer', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'NO', 'column_default' => null, 'udt_name' => 'int4'],
        ]);

        $emptyStmt = static::createStub(StatementInterface::class);
        $emptyStmt->method('fetchAll')->willReturn([]);

        $connection = static::createStub(ConnectionInterface::class);
        $callCount = 0;
        $connection->method('query')->willReturnCallback(
            function () use (&$callCount, $tableStmt, $colStmt, $emptyStmt) {
                $callCount++;

                return match ($callCount) {
                    1 => $tableStmt,
                    2 => $colStmt,
                    3 => $emptyStmt,
                    4 => $emptyStmt,
                    default => false,
                };
            }
        );

        $reflector = new PgSqlSchemaReflector($connection);
        $result = $reflector->reflectAll();

        self::assertSame(
            ['items' => "CREATE TABLE \"items\" (\n  \"id\" INTEGER NOT NULL\n)"],
            $result
        );
    }

    public function testReflectAllSkipsInvalidNames(): void
    {
        $tableStmt = static::createStub(StatementInterface::class);
        $tableStmt->method('fetchAll')->willReturn([
            ['table_name' => ''],
            ['table_name' => null],
            ['other' => 'x'],
        ]);

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
        $colStmt->method('fetchAll')->willReturn([
            ['column_name' => 'id', 'data_type' => 'integer', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'int4'],
        ]);

        $uniqueStmt = static::createStub(StatementInterface::class);
        $uniqueStmt->method('fetchAll')->willReturn([]);

        $connection = static::createStub(ConnectionInterface::class);
        $callCount = 0;
        $connection->method('query')->willReturnCallback(
            function () use (&$callCount, $colStmt, $uniqueStmt) {
                $callCount++;

                return match ($callCount) {
                    1 => $colStmt,
                    2 => false,
                    3 => $uniqueStmt,
                    default => false,
                };
            }
        );

        $reflector = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"t\" (\n  \"id\" INTEGER\n)", $reflector->getCreateStatement('t'));
    }

    public function testUniqueQueryReturnsFalseStillWorks(): void
    {
        $colStmt = static::createStub(StatementInterface::class);
        $colStmt->method('fetchAll')->willReturn([
            ['column_name' => 'id', 'data_type' => 'integer', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'int4'],
        ]);

        $pkStmt = static::createStub(StatementInterface::class);
        $pkStmt->method('fetchAll')->willReturn([]);

        $connection = static::createStub(ConnectionInterface::class);
        $callCount = 0;
        $connection->method('query')->willReturnCallback(
            function () use (&$callCount, $colStmt, $pkStmt) {
                $callCount++;

                return match ($callCount) {
                    1 => $colStmt,
                    2 => $pkStmt,
                    3 => false,
                    default => false,
                };
            }
        );

        $reflector = new PgSqlSchemaReflector($connection);
        self::assertSame("CREATE TABLE \"t\" (\n  \"id\" INTEGER\n)", $reflector->getCreateStatement('t'));
    }

    public function testTableNameEscaping(): void
    {
        $queries = [];

        $colStmt = static::createStub(StatementInterface::class);
        $colStmt->method('fetchAll')->willReturn([]);

        $connection = static::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(
            function (string $q) use (&$queries, $colStmt) {
                $queries[] = $q;

                return $colStmt;
            }
        );

        $reflector = new PgSqlSchemaReflector($connection);
        $reflector->getCreateStatement("it's");

        self::assertStringContainsString("table_name = 'it''s'", $queries[0]);
    }

    public function testNumericStringPrecisionScale(): void
    {
        $r = new PgSqlSchemaReflector(new FakeSequentialConnection([
            new FakeStatement([
                ['column_name' => 'x', 'data_type' => 'numeric', 'character_maximum_length' => null, 'numeric_precision' => '8', 'numeric_scale' => '3', 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'numeric'],
            ]),
            new FakeStatement([]),
            new FakeStatement([]),
        ]));

        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" NUMERIC(8,3)\n)", $r->getCreateStatement('t'));
    }

    public function testVarcharStringLength(): void
    {
        $r = new PgSqlSchemaReflector(new FakeSequentialConnection([
            new FakeStatement([
                ['column_name' => 'x', 'data_type' => 'character varying', 'character_maximum_length' => '100', 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'varchar'],
            ]),
            new FakeStatement([]),
            new FakeStatement([]),
        ]));

        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" VARCHAR(100)\n)", $r->getCreateStatement('t'));
    }

    public function testCharStringLength(): void
    {
        $r = new PgSqlSchemaReflector(new FakeSequentialConnection([
            new FakeStatement([
                ['column_name' => 'x', 'data_type' => 'character', 'character_maximum_length' => '3', 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'bpchar'],
            ]),
            new FakeStatement([]),
            new FakeStatement([]),
        ]));

        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" CHAR(3)\n)", $r->getCreateStatement('t'));
    }

    public function testNumericStringPrecisionOnly(): void
    {
        $r = new PgSqlSchemaReflector(new FakeSequentialConnection([
            new FakeStatement([
                ['column_name' => 'x', 'data_type' => 'numeric', 'character_maximum_length' => null, 'numeric_precision' => '12', 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'numeric'],
            ]),
            new FakeStatement([]),
            new FakeStatement([]),
        ]));

        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" NUMERIC(12)\n)", $r->getCreateStatement('t'));
    }

    public function testMissingColumnNameDefaults(): void
    {
        $r = new PgSqlSchemaReflector(new FakeSequentialConnection([
            new FakeStatement([
                ['data_type' => 'integer', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'int4'],
            ]),
            new FakeStatement([]),
            new FakeStatement([]),
        ]));

        self::assertSame("CREATE TABLE \"t\" (\n  \"\" INTEGER\n)", $r->getCreateStatement('t'));
    }

    public function testMissingDataTypeDefaultsToText(): void
    {
        $r = new PgSqlSchemaReflector(new FakeSequentialConnection([
            new FakeStatement([
                ['column_name' => 'x', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => ''],
            ]),
            new FakeStatement([]),
            new FakeStatement([]),
        ]));

        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" TEXT\n)", $r->getCreateStatement('t'));
    }

    public function testUnknownDataTypePassedThrough(): void
    {
        $r = new PgSqlSchemaReflector(new FakeSequentialConnection([
            new FakeStatement([
                ['column_name' => 'x', 'data_type' => 'xml', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'xml'],
            ]),
            new FakeStatement([]),
            new FakeStatement([]),
        ]));

        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" XML\n)", $r->getCreateStatement('t'));
    }

    public function testReflectAllMultipleTablesExactResults(): void
    {
        $tableStmt = static::createStub(StatementInterface::class);
        $tableStmt->method('fetchAll')->willReturn([
            ['table_name' => 'alpha'],
            ['table_name' => 'beta'],
        ]);

        $colStmtA = static::createStub(StatementInterface::class);
        $colStmtA->method('fetchAll')->willReturn([
            ['column_name' => 'id', 'data_type' => 'integer', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'NO', 'column_default' => null, 'udt_name' => 'int4'],
        ]);

        $colStmtB = static::createStub(StatementInterface::class);
        $colStmtB->method('fetchAll')->willReturn([
            ['column_name' => 'val', 'data_type' => 'text', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'text'],
        ]);

        $emptyStmt = static::createStub(StatementInterface::class);
        $emptyStmt->method('fetchAll')->willReturn([]);

        $connection = static::createStub(ConnectionInterface::class);
        $callCount = 0;
        $connection->method('query')->willReturnCallback(
            function () use (&$callCount, $tableStmt, $colStmtA, $colStmtB, $emptyStmt) {
                $callCount++;

                return match ($callCount) {
                    1 => $tableStmt,
                    2 => $colStmtA,
                    3 => $emptyStmt,
                    4 => $emptyStmt,
                    5 => $colStmtB,
                    6 => $emptyStmt,
                    7 => $emptyStmt,
                    default => false,
                };
            }
        );

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
        $tableStmt->method('fetchAll')->willReturn([
            ['table_name' => 'good'],
            ['table_name' => 'empty_cols'],
        ]);

        $colStmtGood = static::createStub(StatementInterface::class);
        $colStmtGood->method('fetchAll')->willReturn([
            ['column_name' => 'id', 'data_type' => 'integer', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'int4'],
        ]);

        $colStmtEmpty = static::createStub(StatementInterface::class);
        $colStmtEmpty->method('fetchAll')->willReturn([]);

        $emptyStmt = static::createStub(StatementInterface::class);
        $emptyStmt->method('fetchAll')->willReturn([]);

        $connection = static::createStub(ConnectionInterface::class);
        $callCount = 0;
        $connection->method('query')->willReturnCallback(
            function () use (&$callCount, $tableStmt, $colStmtGood, $colStmtEmpty, $emptyStmt) {
                $callCount++;

                return match ($callCount) {
                    1 => $tableStmt,
                    2 => $colStmtGood,
                    3 => $emptyStmt,
                    4 => $emptyStmt,
                    5 => $colStmtEmpty,
                    default => false,
                };
            }
        );

        $reflector = new PgSqlSchemaReflector($connection);
        $result = $reflector->reflectAll();

        self::assertCount(1, $result);
        self::assertArrayHasKey('good', $result);
    }

    public function testNumericNonIntPrecisionCastsToInt(): void
    {
        $r = new PgSqlSchemaReflector(new FakeSequentialConnection([
            new FakeStatement([
                ['column_name' => 'x', 'data_type' => 'numeric', 'character_maximum_length' => null, 'numeric_precision' => 'abc', 'numeric_scale' => 'xyz', 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'numeric'],
            ]),
            new FakeStatement([]),
            new FakeStatement([]),
        ]));

        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" NUMERIC(0,0)\n)", $r->getCreateStatement('t'));
    }

    public function testNumericIntPrecisionAndScaleExactOutput(): void
    {
        $r = new PgSqlSchemaReflector(new FakeSequentialConnection([
            new FakeStatement([
                ['column_name' => 'x', 'data_type' => 'numeric', 'character_maximum_length' => null, 'numeric_precision' => 5, 'numeric_scale' => 3, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'numeric'],
            ]),
            new FakeStatement([]),
            new FakeStatement([]),
        ]));

        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" NUMERIC(5,3)\n)", $r->getCreateStatement('t'));
    }

    public function testNumericPrecisionOnlyNonIntCastsToInt(): void
    {
        $r = new PgSqlSchemaReflector(new FakeSequentialConnection([
            new FakeStatement([
                ['column_name' => 'x', 'data_type' => 'numeric', 'character_maximum_length' => null, 'numeric_precision' => 'bad', 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'numeric'],
            ]),
            new FakeStatement([]),
            new FakeStatement([]),
        ]));

        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" NUMERIC(0)\n)", $r->getCreateStatement('t'));
    }

    public function testArrayWithLowercaseUdtName(): void
    {
        $r = new PgSqlSchemaReflector(new FakeSequentialConnection([
            new FakeStatement([
                ['column_name' => 'x', 'data_type' => 'ARRAY', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => '_text'],
            ]),
            new FakeStatement([]),
            new FakeStatement([]),
        ]));

        self::assertSame("CREATE TABLE \"t\" (\n  \"x\" TEXT[]\n)", $r->getCreateStatement('t'));
    }

    public function testTableNameWithSingleQuoteInReflectAll(): void
    {
        $tableStmt = static::createStub(StatementInterface::class);
        $tableStmt->method('fetchAll')->willReturn([
            ['table_name' => "it's"],
        ]);

        $colStmt = static::createStub(StatementInterface::class);
        $colStmt->method('fetchAll')->willReturn([
            ['column_name' => 'id', 'data_type' => 'integer', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'int4'],
        ]);

        $emptyStmt = static::createStub(StatementInterface::class);
        $emptyStmt->method('fetchAll')->willReturn([]);

        $connection = static::createStub(ConnectionInterface::class);
        $callCount = 0;
        $connection->method('query')->willReturnCallback(
            function (string $q) use (&$callCount, $tableStmt, $colStmt, $emptyStmt) {
                $callCount++;

                return match ($callCount) {
                    1 => $tableStmt,
                    2 => $colStmt,
                    3 => $emptyStmt,
                    4 => $emptyStmt,
                    default => false,
                };
            }
        );

        $reflector = new PgSqlSchemaReflector($connection);
        $result = $reflector->reflectAll();
        self::assertCount(1, $result);
        self::assertArrayHasKey("it's", $result);
    }

    public function testUniqueConstraintNonStringColumnsAreSkipped(): void
    {
        $r = new PgSqlSchemaReflector(new FakeSequentialConnection([
            new FakeStatement([
                ['column_name' => 'id', 'data_type' => 'integer', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'int4'],
            ]),
            new FakeStatement([]),
            new FakeStatement([
                ['constraint_name' => null, 'column_name' => null],
            ]),
        ]));

        self::assertSame("CREATE TABLE \"t\" (\n  \"id\" INTEGER\n)", $r->getCreateStatement('t'));
    }

    public function testPkNonStringColumnNameIsSkipped(): void
    {
        $r = new PgSqlSchemaReflector(new FakeSequentialConnection([
            new FakeStatement([
                ['column_name' => 'id', 'data_type' => 'integer', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'int4'],
            ]),
            new FakeStatement([
                ['column_name' => null],
            ]),
            new FakeStatement([]),
        ]));

        self::assertSame("CREATE TABLE \"t\" (\n  \"id\" INTEGER\n)", $r->getCreateStatement('t'));
    }
}
