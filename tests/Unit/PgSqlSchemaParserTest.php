<?php

declare(strict_types=1);

namespace Tests\Unit;

use Tests\Contract\SchemaParserContractTest;
use ZtdQuery\Platform\Postgres\PgSqlSchemaParser;
use ZtdQuery\Platform\SchemaParser;
use ZtdQuery\Schema\ColumnTypeFamily;
use PHPUnit\Framework\Attributes\CoversClass;

#[CoversClass(PgSqlSchemaParser::class)]
final class PgSqlSchemaParserTest extends SchemaParserContractTest
{
    protected function createParser(): SchemaParser
    {
        return new PgSqlSchemaParser();
    }

    protected function validCreateTableSql(): string
    {
        return <<<'SQL'
            CREATE TABLE users (
                id INTEGER NOT NULL,
                name VARCHAR(255) NOT NULL,
                email TEXT,
                PRIMARY KEY (id),
                UNIQUE (email)
            )
            SQL;
    }

    protected function nonCreateTableSql(): string
    {
        return 'SELECT 1';
    }

    public function testParseBasicCreateTable(): void
    {
        $parser = new PgSqlSchemaParser();
        $sql = 'CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            email TEXT
        )';

        $def = $parser->parse($sql);

        self::assertNotNull($def);
        self::assertSame(['id', 'name', 'email'], $def->columns);
        self::assertSame(['id'], $def->primaryKeys);
        self::assertContains('id', $def->notNullColumns);
        self::assertContains('name', $def->notNullColumns);
    }

    public function testParseColumnTypes(): void
    {
        $parser = new PgSqlSchemaParser();
        $sql = 'CREATE TABLE test (
            id INTEGER,
            price NUMERIC(10,2),
            name VARCHAR(100),
            active BOOLEAN,
            created_at TIMESTAMP,
            data JSONB
        )';

        $def = $parser->parse($sql);

        self::assertNotNull($def);
        self::assertSame('INTEGER', $def->columnTypes['id']);
        self::assertSame('NUMERIC(10,2)', $def->columnTypes['price']);
        self::assertSame('VARCHAR(100)', $def->columnTypes['name']);
        self::assertSame('BOOLEAN', $def->columnTypes['active']);
        self::assertSame('TIMESTAMP', $def->columnTypes['created_at']);
        self::assertSame('JSONB', $def->columnTypes['data']);
    }

    public function testParseTypedColumns(): void
    {
        $parser = new PgSqlSchemaParser();
        $sql = 'CREATE TABLE test (
            id INTEGER,
            name TEXT,
            active BOOLEAN,
            data JSONB
        )';

        $def = $parser->parse($sql);

        self::assertNotNull($def);
        self::assertSame(ColumnTypeFamily::INTEGER, $def->typedColumns['id']->family);
        self::assertSame(ColumnTypeFamily::TEXT, $def->typedColumns['name']->family);
        self::assertSame(ColumnTypeFamily::BOOLEAN, $def->typedColumns['active']->family);
        self::assertSame(ColumnTypeFamily::JSON, $def->typedColumns['data']->family);
    }

    public function testParsePrimaryKeyConstraint(): void
    {
        $parser = new PgSqlSchemaParser();
        $sql = 'CREATE TABLE test (
            id INTEGER,
            name TEXT,
            PRIMARY KEY (id)
        )';

        $def = $parser->parse($sql);

        self::assertNotNull($def);
        self::assertSame(['id'], $def->primaryKeys);
    }

    public function testParseCompositePrimaryKey(): void
    {
        $parser = new PgSqlSchemaParser();
        $sql = 'CREATE TABLE test (
            user_id INTEGER,
            role_id INTEGER,
            PRIMARY KEY (user_id, role_id)
        )';

        $def = $parser->parse($sql);

        self::assertNotNull($def);
        self::assertSame(['user_id', 'role_id'], $def->primaryKeys);
    }

    public function testParseUniqueConstraint(): void
    {
        $parser = new PgSqlSchemaParser();
        $sql = 'CREATE TABLE test (
            id INTEGER PRIMARY KEY,
            email TEXT,
            CONSTRAINT email_unique UNIQUE (email)
        )';

        $def = $parser->parse($sql);

        self::assertNotNull($def);
        self::assertArrayHasKey('email_unique', $def->uniqueConstraints);
        self::assertSame(['email'], $def->uniqueConstraints['email_unique']);
    }

    public function testParseUniqueOnColumn(): void
    {
        $parser = new PgSqlSchemaParser();
        $sql = 'CREATE TABLE test (
            id INTEGER PRIMARY KEY,
            email TEXT UNIQUE
        )';

        $def = $parser->parse($sql);

        self::assertNotNull($def);
        self::assertArrayHasKey('email_UNIQUE', $def->uniqueConstraints);
    }

    public function testParseQuotedIdentifiers(): void
    {
        $parser = new PgSqlSchemaParser();
        $sql = 'CREATE TABLE "Users" (
            "Id" INTEGER PRIMARY KEY,
            "Name" VARCHAR(255) NOT NULL
        )';

        $def = $parser->parse($sql);

        self::assertNotNull($def);
        self::assertSame(['Id', 'Name'], $def->columns);
    }

    public function testParseIfNotExists(): void
    {
        $parser = new PgSqlSchemaParser();
        $sql = 'CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY
        )';

        $def = $parser->parse($sql);
        self::assertNotNull($def);
    }

    public function testParseTemporaryTable(): void
    {
        $parser = new PgSqlSchemaParser();
        $sql = 'CREATE TEMPORARY TABLE tmp (
            id INTEGER PRIMARY KEY
        )';

        $def = $parser->parse($sql);
        self::assertNotNull($def);
    }

    public function testNonCreateTableReturnsNull(): void
    {
        $parser = new PgSqlSchemaParser();
        self::assertNull($parser->parse('SELECT * FROM users'));
        self::assertNull($parser->parse('INSERT INTO users (id) VALUES (1)'));
        self::assertNull($parser->parse('DROP TABLE users'));
    }

    public function testMalformedSqlReturnsNull(): void
    {
        $parser = new PgSqlSchemaParser();
        self::assertNull($parser->parse('GIBBERISH'));
    }

    public function testPrimaryKeysSubsetOfColumns(): void
    {
        $parser = new PgSqlSchemaParser();
        $sql = 'CREATE TABLE test (
            id INTEGER,
            name TEXT,
            PRIMARY KEY (id)
        )';

        $def = $parser->parse($sql);
        self::assertNotNull($def);
        self::assertSame(['id'], $def->primaryKeys);
        self::assertContains('id', $def->columns);
    }

    public function testNotNullSubsetOfColumns(): void
    {
        $parser = new PgSqlSchemaParser();
        $sql = 'CREATE TABLE test (
            id INTEGER NOT NULL,
            name TEXT
        )';

        $def = $parser->parse($sql);
        self::assertNotNull($def);
        self::assertContains('id', $def->notNullColumns);
        self::assertContains('id', $def->columns);
    }

    public function testColumnTypesKeysSubsetOfColumns(): void
    {
        $parser = new PgSqlSchemaParser();
        $sql = 'CREATE TABLE test (
            id INTEGER,
            name TEXT
        )';

        $def = $parser->parse($sql);
        self::assertNotNull($def);
        self::assertArrayHasKey('id', $def->columnTypes);
        self::assertContains('id', $def->columns);
        self::assertArrayHasKey('name', $def->columnTypes);
        self::assertContains('name', $def->columns);
    }

    public function testParseSerialType(): void
    {
        $parser = new PgSqlSchemaParser();
        $sql = 'CREATE TABLE test (
            id SERIAL PRIMARY KEY,
            name TEXT
        )';

        $def = $parser->parse($sql);

        self::assertNotNull($def);
        self::assertSame(ColumnTypeFamily::INTEGER, $def->typedColumns['id']->family);
    }

    public function testParseBigserialType(): void
    {
        $parser = new PgSqlSchemaParser();
        $sql = 'CREATE TABLE test (
            id BIGSERIAL PRIMARY KEY
        )';

        $def = $parser->parse($sql);

        self::assertNotNull($def);
        self::assertSame(ColumnTypeFamily::INTEGER, $def->typedColumns['id']->family);
    }

    public function testParseTimestamptzType(): void
    {
        $parser = new PgSqlSchemaParser();
        $sql = 'CREATE TABLE test (
            created_at TIMESTAMPTZ
        )';

        $def = $parser->parse($sql);

        self::assertNotNull($def);
        self::assertSame(ColumnTypeFamily::TIMESTAMP, $def->typedColumns['created_at']->family);
    }

    public function testParseByteaType(): void
    {
        $parser = new PgSqlSchemaParser();
        $sql = 'CREATE TABLE test (
            data BYTEA
        )';

        $def = $parser->parse($sql);

        self::assertNotNull($def);
        self::assertSame(ColumnTypeFamily::BINARY, $def->typedColumns['data']->family);
    }

    public function testParseSchemaQualifiedTable(): void
    {
        $parser = new PgSqlSchemaParser();
        $sql = 'CREATE TABLE public.users (
            id INTEGER PRIMARY KEY
        )';

        $def = $parser->parse($sql);
        self::assertNotNull($def);
        self::assertSame(['id'], $def->columns);
    }

    public function testParseReservedWordColumnNames(): void
    {
        $parser = new PgSqlSchemaParser();
        $sql = 'CREATE TABLE test (
            "select" INTEGER,
            "table" TEXT,
            "order" INTEGER
        )';

        $def = $parser->parse($sql);

        self::assertNotNull($def);
        self::assertSame(['select', 'table', 'order'], $def->columns);
    }

    public function testParseEmptyInputReturnsNull(): void
    {
        $parser = new PgSqlSchemaParser();
        self::assertNull($parser->parse(''));
    }

    public function testParseDefaultValues(): void
    {
        $parser = new PgSqlSchemaParser();
        $sql = 'CREATE TABLE test (
            id INTEGER PRIMARY KEY,
            status TEXT DEFAULT \'active\' NOT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        )';

        $def = $parser->parse($sql);

        self::assertNotNull($def);
        self::assertSame(['id', 'status', 'created_at'], $def->columns);
        self::assertContains('status', $def->notNullColumns);
    }

    public function testParseMultipleUniqueConstraints(): void
    {
        $parser = new PgSqlSchemaParser();
        $sql = 'CREATE TABLE test (
            id INTEGER PRIMARY KEY,
            email TEXT,
            username TEXT,
            CONSTRAINT email_unique UNIQUE (email),
            CONSTRAINT username_unique UNIQUE (username)
        )';

        $def = $parser->parse($sql);

        self::assertNotNull($def);
        self::assertArrayHasKey('email_unique', $def->uniqueConstraints);
        self::assertArrayHasKey('username_unique', $def->uniqueConstraints);
        self::assertSame(['email'], $def->uniqueConstraints['email_unique']);
        self::assertSame(['username'], $def->uniqueConstraints['username_unique']);
    }

    public function testParseDoublePrecision(): void
    {
        $parser = new PgSqlSchemaParser();
        $sql = 'CREATE TABLE t (v DOUBLE PRECISION)';
        $def = $parser->parse($sql);
        self::assertNotNull($def);
        self::assertSame('DOUBLE PRECISION', $def->columnTypes['v']);
        self::assertSame(ColumnTypeFamily::DOUBLE, $def->typedColumns['v']->family);
    }

    public function testParseFloat4(): void
    {
        $parser = new PgSqlSchemaParser();
        $sql = 'CREATE TABLE t (v FLOAT4)';
        $def = $parser->parse($sql);
        self::assertNotNull($def);
        self::assertSame(ColumnTypeFamily::FLOAT, $def->typedColumns['v']->family);
    }

    public function testParseFloat8(): void
    {
        $parser = new PgSqlSchemaParser();
        $sql = 'CREATE TABLE t (v FLOAT8)';
        $def = $parser->parse($sql);
        self::assertNotNull($def);
        self::assertSame(ColumnTypeFamily::DOUBLE, $def->typedColumns['v']->family);
    }

    public function testParseRealType(): void
    {
        $parser = new PgSqlSchemaParser();
        $sql = 'CREATE TABLE t (v REAL)';
        $def = $parser->parse($sql);
        self::assertNotNull($def);
        self::assertSame(ColumnTypeFamily::FLOAT, $def->typedColumns['v']->family);
    }

    public function testParseDecimalType(): void
    {
        $parser = new PgSqlSchemaParser();
        $sql = 'CREATE TABLE t (v DECIMAL(10,2))';
        $def = $parser->parse($sql);
        self::assertNotNull($def);
        self::assertSame('DECIMAL(10,2)', $def->columnTypes['v']);
        self::assertSame(ColumnTypeFamily::DECIMAL, $def->typedColumns['v']->family);
    }

    public function testParseSmallint(): void
    {
        $parser = new PgSqlSchemaParser();
        $sql = 'CREATE TABLE t (v SMALLINT)';
        $def = $parser->parse($sql);
        self::assertNotNull($def);
        self::assertSame(ColumnTypeFamily::INTEGER, $def->typedColumns['v']->family);
    }

    public function testParseBigint(): void
    {
        $parser = new PgSqlSchemaParser();
        $sql = 'CREATE TABLE t (v BIGINT)';
        $def = $parser->parse($sql);
        self::assertNotNull($def);
        self::assertSame(ColumnTypeFamily::INTEGER, $def->typedColumns['v']->family);
    }

    public function testParseSmallserial(): void
    {
        $parser = new PgSqlSchemaParser();
        $sql = 'CREATE TABLE t (id SMALLSERIAL)';
        $def = $parser->parse($sql);
        self::assertNotNull($def);
        self::assertSame(ColumnTypeFamily::INTEGER, $def->typedColumns['id']->family);
    }

    public function testParseInt2Int4Int8(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (a INT2, b INT4, c INT8)');
        self::assertNotNull($def);
        self::assertSame(ColumnTypeFamily::INTEGER, $def->typedColumns['a']->family);
        self::assertSame(ColumnTypeFamily::INTEGER, $def->typedColumns['b']->family);
        self::assertSame(ColumnTypeFamily::INTEGER, $def->typedColumns['c']->family);
    }

    public function testParseDateType(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (v DATE)');
        self::assertNotNull($def);
        self::assertSame(ColumnTypeFamily::DATE, $def->typedColumns['v']->family);
    }

    public function testParseTimeType(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (v TIME)');
        self::assertNotNull($def);
        self::assertSame(ColumnTypeFamily::TIME, $def->typedColumns['v']->family);
    }

    public function testParseTimetzType(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (v TIMETZ)');
        self::assertNotNull($def);
        self::assertSame(ColumnTypeFamily::TIME, $def->typedColumns['v']->family);
    }

    public function testParseTimeWithTimezoneMultiWord(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (v TIME WITH TIME ZONE)');
        self::assertNotNull($def);
        self::assertSame('TIME WITH', $def->columnTypes['v']);
    }

    public function testParseTimestampWithoutTimezoneMultiWord(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (v TIMESTAMP WITHOUT TIME ZONE)');
        self::assertNotNull($def);
        self::assertSame('TIMESTAMP WITHOUT', $def->columnTypes['v']);
    }

    public function testParseTimestampWithTimezoneMultiWord(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (v TIMESTAMP WITH TIME ZONE)');
        self::assertNotNull($def);
        self::assertSame('TIMESTAMP WITH', $def->columnTypes['v']);
    }

    public function testParseJsonType(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (v JSON)');
        self::assertNotNull($def);
        self::assertSame(ColumnTypeFamily::JSON, $def->typedColumns['v']->family);
    }

    public function testParseBoolType(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (v BOOL)');
        self::assertNotNull($def);
        self::assertSame(ColumnTypeFamily::BOOLEAN, $def->typedColumns['v']->family);
    }

    public function testParseCharType(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (v CHAR(5))');
        self::assertNotNull($def);
        self::assertSame('CHAR(5)', $def->columnTypes['v']);
        self::assertSame(ColumnTypeFamily::STRING, $def->typedColumns['v']->family);
    }

    public function testParseCharacterType(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (v CHARACTER(10))');
        self::assertNotNull($def);
        self::assertSame(ColumnTypeFamily::STRING, $def->typedColumns['v']->family);
    }

    public function testParseCharacterVaryingType(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (v CHARACTER VARYING(50))');
        self::assertNotNull($def);
        self::assertSame('CHARACTER VARYING(50)', $def->columnTypes['v']);
        self::assertSame(ColumnTypeFamily::STRING, $def->typedColumns['v']->family);
    }

    public function testParseNameType(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (v NAME)');
        self::assertNotNull($def);
        self::assertSame(ColumnTypeFamily::STRING, $def->typedColumns['v']->family);
    }

    public function testParseCitextType(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (v CITEXT)');
        self::assertNotNull($def);
        self::assertSame(ColumnTypeFamily::TEXT, $def->typedColumns['v']->family);
    }

    public function testParseVarcharType(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (v VARCHAR)');
        self::assertNotNull($def);
        self::assertSame(ColumnTypeFamily::STRING, $def->typedColumns['v']->family);
    }

    public function testParseUnknownType(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (v GEOMETRY)');
        self::assertNotNull($def);
        self::assertSame(ColumnTypeFamily::UNKNOWN, $def->typedColumns['v']->family);
    }

    public function testParseArrayType(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (v INTEGER[])');
        self::assertNotNull($def);
        self::assertSame('INTEGER[]', $def->columnTypes['v']);
        self::assertSame(ColumnTypeFamily::INTEGER, $def->typedColumns['v']->family);
    }

    public function testParsePrimaryKeyImpliesNotNull(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (id INTEGER PRIMARY KEY)');
        self::assertNotNull($def);
        self::assertContains('id', $def->notNullColumns);
        self::assertSame(['id'], $def->primaryKeys);
    }

    public function testParsePrimaryKeyConstraintDoesNotDuplicate(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (id INTEGER PRIMARY KEY, PRIMARY KEY (id))');
        self::assertNotNull($def);
        self::assertSame(['id'], $def->primaryKeys);
    }

    public function testParseUniqueConstraintWithoutName(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (id INTEGER, email TEXT, UNIQUE (email))');
        self::assertNotNull($def);
        self::assertContains(['email'], $def->uniqueConstraints);
    }

    public function testParseCheckConstraintIgnored(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (id INTEGER, CHECK (id > 0))');
        self::assertNotNull($def);
        self::assertSame(['id'], $def->columns);
    }

    public function testParseForeignKeyConstraintIgnored(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (id INTEGER, FOREIGN KEY (id) REFERENCES other(id))');
        self::assertNotNull($def);
        self::assertSame(['id'], $def->columns);
    }

    public function testParseExcludeConstraintIgnored(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (id INTEGER, EXCLUDE USING gist (id WITH =))');
        self::assertNotNull($def);
        self::assertSame(['id'], $def->columns);
    }

    public function testParseColumnDefaultNotNull(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse("CREATE TABLE t (s TEXT NOT NULL DEFAULT 'active')");
        self::assertNotNull($def);
        self::assertContains('s', $def->notNullColumns);
    }

    public function testParseUnloggedTable(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE UNLOGGED TABLE t (id INTEGER)');
        self::assertNotNull($def);
        self::assertSame(['id'], $def->columns);
    }

    public function testParseTempTable(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TEMP TABLE t (id INTEGER)');
        self::assertNotNull($def);
        self::assertSame(['id'], $def->columns);
    }

    public function testParseCompositeUniqueConstraint(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (a INTEGER, b INTEGER, CONSTRAINT uq_ab UNIQUE (a, b))');
        self::assertNotNull($def);
        self::assertSame(['a', 'b'], $def->uniqueConstraints['uq_ab']);
    }

    public function testParseColumnTypeUppercased(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (v integer not null)');
        self::assertNotNull($def);
        self::assertSame('INTEGER', $def->columnTypes['v']);
        self::assertContains('v', $def->notNullColumns);
    }

    public function testParseUniqueConstraintReferencingNonExistentColumnReturnsNull(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (id INTEGER, CONSTRAINT uq UNIQUE (nonexistent))');
        self::assertNull($def);
    }

    public function testParseTimestamptzColumnType(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (v TIMESTAMPTZ)');
        self::assertNotNull($def);
        self::assertSame('TIMESTAMPTZ', $def->columnTypes['v']);
    }

    public function testParseEscapedDoubleQuoteIdentifierReturnsNull(): void
    {
        $parser = new PgSqlSchemaParser();
        self::assertNull($parser->parse('CREATE TABLE t ("a""b" INTEGER)'));
    }

    public function testParseDefaultWithParentheses(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (v TIMESTAMP DEFAULT NOW(), id INTEGER)');
        self::assertNotNull($def);
        self::assertSame(['v', 'id'], $def->columns);
    }

    public function testParseColumnWithSingleQuoteDefault(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse("CREATE TABLE t (v TEXT DEFAULT 'a,b', id INTEGER)");
        self::assertNotNull($def);
        self::assertSame(['v', 'id'], $def->columns);
    }

    public function testParseIntType(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (v INT)');
        self::assertNotNull($def);
        self::assertSame(ColumnTypeFamily::INTEGER, $def->typedColumns['v']->family);
        self::assertSame('INT', $def->columnTypes['v']);
    }

    public function testParseTimeWithoutTimezoneMultiWord(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (v TIME WITHOUT TIME ZONE)');
        self::assertNotNull($def);
        self::assertSame('TIME WITHOUT', $def->columnTypes['v']);
    }

    public function testParseColumnWithGeneratedClause(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (id INTEGER GENERATED ALWAYS AS IDENTITY)');
        self::assertNotNull($def);
        self::assertSame(['id'], $def->columns);
    }

    public function testParseMultiWordTypeNotConflictWithConstraint(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (v DOUBLE PRECISION NOT NULL)');
        self::assertNotNull($def);
        self::assertSame('DOUBLE PRECISION', $def->columnTypes['v']);
        self::assertContains('v', $def->notNullColumns);
    }

    public function testParseBitType(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (v BIT(8))');
        self::assertNotNull($def);
        self::assertSame('BIT(8)', $def->columnTypes['v']);
    }

    public function testParseIntervalType(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (v INTERVAL)');
        self::assertNotNull($def);
        self::assertSame('INTERVAL', $def->columnTypes['v']);
    }

    public function testParseColumnNamedConstraintKeyword(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t ("primary" INTEGER, "check" TEXT)');
        self::assertNotNull($def);
        self::assertSame(['primary', 'check'], $def->columns);
    }

    public function testParseNoColumnsReturnsNull(): void
    {
        $parser = new PgSqlSchemaParser();
        self::assertNull($parser->parse('CREATE TABLE t (PRIMARY KEY (id))'));
    }

    public function testParseColumnNativeTypeIsUpper(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (v varchar(50))');
        self::assertNotNull($def);
        self::assertSame('VARCHAR(50)', $def->columnTypes['v']);
        self::assertSame('VARCHAR(50)', $def->typedColumns['v']->nativeType);
    }

    public function testParseMultilineCreateTable(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse("CREATE TABLE t (\nid INTEGER,\nname TEXT\n)");
        self::assertNotNull($def);
        self::assertSame(['id', 'name'], $def->columns);
    }

    public function testParseLowercaseCreateTable(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('create table t (id integer, name text)');
        self::assertNotNull($def);
        self::assertSame(['id', 'name'], $def->columns);
    }

    public function testParseLowercasePrimaryKey(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('create table t (id integer, name text, primary key (id))');
        self::assertNotNull($def);
        self::assertSame(['id'], $def->primaryKeys);
    }

    public function testParseLowercaseUnique(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('create table t (id integer, email text, unique (email))');
        self::assertNotNull($def);
        self::assertNotEmpty($def->uniqueConstraints);
    }

    public function testParseLowercaseNotNull(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('create table t (id integer not null, name text)');
        self::assertNotNull($def);
        self::assertContains('id', $def->notNullColumns);
    }

    public function testParseLowercaseInlinePrimaryKey(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('create table t (id integer primary key, name text)');
        self::assertNotNull($def);
        self::assertSame(['id'], $def->primaryKeys);
    }

    public function testParseLowercaseInlineUnique(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('create table t (id integer, email text unique)');
        self::assertNotNull($def);
        self::assertNotEmpty($def->uniqueConstraints);
    }

    public function testParseMultilineColumnDefinition(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse("CREATE TABLE t (\nid\nINTEGER\nNOT NULL,\nname TEXT\n)");
        self::assertNotNull($def);
        self::assertSame(['id', 'name'], $def->columns);
        self::assertContains('id', $def->notNullColumns);
    }

    public function testParseColumnWithLeadingWhitespace(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse("CREATE TABLE t (  id INTEGER,  name TEXT  )");
        self::assertNotNull($def);
        self::assertSame(['id', 'name'], $def->columns);
    }

    public function testParseDoubleQuotedColumnName(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t ("Column Name" TEXT, id INTEGER)');
        self::assertNotNull($def);
        self::assertContains('Column Name', $def->columns);
    }

    public function testParseCharTypeNoLength(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (c CHAR)');
        self::assertNotNull($def);
        self::assertSame('CHAR', $def->columnTypes['c']);
        self::assertSame(ColumnTypeFamily::STRING, $def->typedColumns['c']->family);
    }

    public function testParseCharTypeWithLength(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (c CHAR(5))');
        self::assertNotNull($def);
        self::assertSame('CHAR(5)', $def->columnTypes['c']);
    }

    public function testParseVarcharNoLength(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (v VARCHAR)');
        self::assertNotNull($def);
        self::assertSame('VARCHAR', $def->columnTypes['v']);
    }

    public function testParseNumericWithPrecisionOnly(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (n NUMERIC(10))');
        self::assertNotNull($def);
        self::assertSame('NUMERIC(10)', $def->columnTypes['n']);
    }

    public function testParseNumericNoParams(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (n NUMERIC)');
        self::assertNotNull($def);
        self::assertSame('NUMERIC', $def->columnTypes['n']);
    }

    public function testParseDoublePrecisionType(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (d DOUBLE PRECISION)');
        self::assertNotNull($def);
        self::assertSame('DOUBLE PRECISION', $def->columnTypes['d']);
        self::assertSame(ColumnTypeFamily::DOUBLE, $def->typedColumns['d']->family);
    }

    public function testParseTimestampType(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (ts TIMESTAMP)');
        self::assertNotNull($def);
        self::assertSame(ColumnTypeFamily::TIMESTAMP, $def->typedColumns['ts']->family);
    }

    public function testParseBooleanType(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (b BOOLEAN)');
        self::assertNotNull($def);
        self::assertSame(ColumnTypeFamily::BOOLEAN, $def->typedColumns['b']->family);
    }

    public function testParseJsonbType(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (j JSONB)');
        self::assertNotNull($def);
        self::assertSame(ColumnTypeFamily::JSON, $def->typedColumns['j']->family);
    }

    public function testParseSmallintType(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (s SMALLINT)');
        self::assertNotNull($def);
        self::assertSame(ColumnTypeFamily::INTEGER, $def->typedColumns['s']->family);
    }

    public function testParseBigintType(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (b BIGINT)');
        self::assertNotNull($def);
        self::assertSame(ColumnTypeFamily::INTEGER, $def->typedColumns['b']->family);
    }

    public function testParseTextType(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (t TEXT)');
        self::assertNotNull($def);
        self::assertSame(ColumnTypeFamily::TEXT, $def->typedColumns['t']->family);
    }

    public function testParseMultiDimensionalArray(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (a INTEGER[][])');
        self::assertNotNull($def);
        self::assertSame('INTEGER[][]', $def->columnTypes['a']);
    }

    public function testParseNamedConstraintPrimaryKey(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (id INTEGER, CONSTRAINT pk_t PRIMARY KEY (id))');
        self::assertNotNull($def);
        self::assertSame(['id'], $def->primaryKeys);
    }

    public function testParseNamedConstraintUnique(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (id INTEGER, email TEXT, CONSTRAINT uq_email UNIQUE (email))');
        self::assertNotNull($def);
        self::assertArrayHasKey('uq_email', $def->uniqueConstraints);
        self::assertSame(['email'], $def->uniqueConstraints['uq_email']);
    }

    public function testParseConstraintEntryCheck(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (id INTEGER, CHECK (id > 0))');
        self::assertNotNull($def);
        self::assertSame(['id'], $def->columns);
    }

    public function testParseConstraintEntryForeignKey(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (id INTEGER, FOREIGN KEY (id) REFERENCES other(id))');
        self::assertNotNull($def);
        self::assertSame(['id'], $def->columns);
    }

    public function testParseConstraintEntryExclude(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (id INTEGER, EXCLUDE USING gist (id WITH =))');
        self::assertNotNull($def);
        self::assertSame(['id'], $def->columns);
    }

    public function testParseWithLeadingWhitespace(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('   CREATE TABLE t (id INTEGER)');
        self::assertNotNull($def);
        self::assertSame(['id'], $def->columns);
    }

    public function testParseNonCreateTableReturnsNull(): void
    {
        $parser = new PgSqlSchemaParser();
        self::assertNull($parser->parse('SELECT 1'));
    }

    public function testParseEmptyBody(): void
    {
        $parser = new PgSqlSchemaParser();
        self::assertNull($parser->parse('CREATE TABLE t ()'));
    }

    public function testParseSingleQuoteInDefaultValue(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse("CREATE TABLE t (name TEXT DEFAULT 'it''s', id INTEGER)");
        self::assertNotNull($def);
        self::assertSame(['name', 'id'], $def->columns);
    }

    public function testParseDoubleQuotedColumnWithComma(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t ("a,b" TEXT, id INTEGER)');
        self::assertNotNull($def);
        self::assertContains('a,b', $def->columns);
    }

    public function testParsePrimaryKeyAddsToNotNull(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)');
        self::assertNotNull($def);
        self::assertContains('id', $def->notNullColumns);
        self::assertContains('id', $def->primaryKeys);
    }

    public function testParseUniqueConstraintWithNonExistentColumnReturnsNull(): void
    {
        $parser = new PgSqlSchemaParser();
        self::assertNull($parser->parse('CREATE TABLE t (id INTEGER, UNIQUE (nonexistent))'));
    }

    public function testParseIfNotExistsTable(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE IF NOT EXISTS t (id INTEGER)');
        self::assertNotNull($def);
        self::assertSame(['id'], $def->columns);
    }

    public function testParseLowercaseTimestamp(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('create table t (ts timestamp)');
        self::assertNotNull($def);
        self::assertSame(ColumnTypeFamily::TIMESTAMP, $def->typedColumns['ts']->family);
    }

    public function testParseLowercaseBoolean(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('create table t (b boolean)');
        self::assertNotNull($def);
        self::assertSame(ColumnTypeFamily::BOOLEAN, $def->typedColumns['b']->family);
    }

    public function testParseDoubleQuotedTableName(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE "My Table" (id INTEGER)');
        self::assertNotNull($def);
        self::assertSame(['id'], $def->columns);
    }

    public function testParseInt2Type(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (n INT2)');
        self::assertNotNull($def);
        self::assertSame(ColumnTypeFamily::INTEGER, $def->typedColumns['n']->family);
    }

    public function testParseInt4Type(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (n INT4)');
        self::assertNotNull($def);
        self::assertSame(ColumnTypeFamily::INTEGER, $def->typedColumns['n']->family);
    }

    public function testParseInt8Type(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (n INT8)');
        self::assertNotNull($def);
        self::assertSame(ColumnTypeFamily::INTEGER, $def->typedColumns['n']->family);
    }

    public function testParseFloat4Type(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (f FLOAT4)');
        self::assertNotNull($def);
        self::assertSame(ColumnTypeFamily::FLOAT, $def->typedColumns['f']->family);
    }

    public function testParseFloat8Type(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (f FLOAT8)');
        self::assertNotNull($def);
        self::assertSame(ColumnTypeFamily::DOUBLE, $def->typedColumns['f']->family);
    }

    public function testParseSmallserialType(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (s SMALLSERIAL)');
        self::assertNotNull($def);
        self::assertSame(ColumnTypeFamily::INTEGER, $def->typedColumns['s']->family);
    }

    public function testParseColumnTypeWithWhitespaceBeforeParams(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (v VARCHAR (50))');
        self::assertNotNull($def);
        self::assertSame('VARCHAR(50)', $def->columnTypes['v']);
    }

    public function testParseMultiWordTypePrefix(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (d DOUBLE PRECISION, t TIMESTAMP)');
        self::assertNotNull($def);
        self::assertSame('DOUBLE PRECISION', $def->columnTypes['d']);
        self::assertSame('TIMESTAMP', $def->columnTypes['t']);
    }

    public function testParseMultiWordTypeWithConstraintAfter(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (d DOUBLE PRECISION NOT NULL)');
        self::assertNotNull($def);
        self::assertSame('DOUBLE PRECISION', $def->columnTypes['d']);
        self::assertContains('d', $def->notNullColumns);
    }

    public function testParseTimestampMultiWordFollowedByConstraint(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (ts TIMESTAMP NOT NULL)');
        self::assertNotNull($def);
        self::assertSame('TIMESTAMP', $def->columnTypes['ts']);
        self::assertContains('ts', $def->notNullColumns);
    }

    public function testParseColumnWithDefaultAndNotNull(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (active BOOLEAN DEFAULT TRUE NOT NULL)');
        self::assertNotNull($def);
        self::assertContains('active', $def->notNullColumns);
    }

    public function testParseLowercaseDoublePrecision(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('create table t (d double precision)');
        self::assertNotNull($def);
        self::assertSame('DOUBLE PRECISION', $def->columnTypes['d']);
    }

    public function testParseCompositeUnique(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (a INTEGER, b INTEGER, UNIQUE (a, b))');
        self::assertNotNull($def);
        $uk = array_values($def->uniqueConstraints);
        self::assertSame(['a', 'b'], $uk[0]);
    }

    public function testParsePrimaryKeyNoDuplicate(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (id INTEGER PRIMARY KEY, PRIMARY KEY (id))');
        self::assertNotNull($def);
        self::assertCount(1, $def->primaryKeys);
    }

    public function testParseInlineUniqueColumnNameFormat(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (email TEXT UNIQUE)');
        self::assertNotNull($def);
        self::assertArrayHasKey('email_UNIQUE', $def->uniqueConstraints);
        self::assertSame(['email'], $def->uniqueConstraints['email_UNIQUE']);
    }

    public function testParseColumnDefinitionWithMultilineTypeAndConstraints(): void
    {
        $parser = new PgSqlSchemaParser();
        $sql = "CREATE TABLE t (bio TEXT\nNOT NULL\nUNIQUE)";
        $def = $parser->parse($sql);
        self::assertNotNull($def);
        self::assertContains('bio', $def->notNullColumns);
        self::assertArrayHasKey('bio_UNIQUE', $def->uniqueConstraints);
    }

    public function testParseLowercaseConstraintPrimaryKey(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (id integer, primary key (id))');
        self::assertNotNull($def);
        self::assertSame(['id'], $def->primaryKeys);
    }

    public function testParseLowercaseConstraintUnique(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (email text, unique (email))');
        self::assertNotNull($def);
        $uk = array_values($def->uniqueConstraints);
        self::assertSame(['email'], $uk[0]);
    }

    public function testParseLowercaseConstraintKeyword(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (id integer, constraint pk_t primary key (id))');
        self::assertNotNull($def);
        self::assertSame(['id'], $def->primaryKeys);
    }

    public function testParseArrayTypeColumn(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (tags TEXT[])');
        self::assertNotNull($def);
        self::assertSame(['tags'], $def->columns);
        self::assertSame('TEXT[]', $def->columnTypes['tags']);
    }

    public function testParseMultiWordTypeWithConstraintKeyword(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (ts TIMESTAMP NOT NULL)');
        self::assertNotNull($def);
        self::assertSame('TIMESTAMP', $def->columnTypes['ts']);
        self::assertContains('ts', $def->notNullColumns);
    }

    public function testParseMultiWordTypeTimestampWithTimeZone(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (d DOUBLE PRECISION, ts TIMESTAMP)');
        self::assertNotNull($def);
        self::assertSame('DOUBLE PRECISION', $def->columnTypes['d']);
    }

    public function testParseEmptyEntrySkipped(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (id INTEGER, , name TEXT)');
        self::assertNotNull($def);
        self::assertSame(['id', 'name'], $def->columns);
    }

    public function testParseQuotedColumnName(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t ("My Column" TEXT)');
        self::assertNotNull($def);
        self::assertSame(['My Column'], $def->columns);
    }

    public function testParseDefaultValueWithStringContainingComma(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse("CREATE TABLE t (id INTEGER, note TEXT DEFAULT 'a,b,c')");
        self::assertNotNull($def);
        self::assertSame(['id', 'note'], $def->columns);
    }

    public function testParseCheckConstraintSkipped(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (age INTEGER, CHECK (age > 0))');
        self::assertNotNull($def);
        self::assertSame(['age'], $def->columns);
    }

    public function testParseForeignKeyConstraintSkipped(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (user_id INTEGER, FOREIGN KEY (user_id) REFERENCES users(id))');
        self::assertNotNull($def);
        self::assertSame(['user_id'], $def->columns);
    }

    public function testParseExcludeConstraintSkipped(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (tsrange TSRANGE, EXCLUDE USING gist (tsrange WITH &&))');
        self::assertNotNull($def);
        self::assertSame(['tsrange'], $def->columns);
    }

    public function testParseInvalidSqlReturnsNull(): void
    {
        $parser = new PgSqlSchemaParser();
        self::assertNull($parser->parse('NOT A CREATE TABLE'));
    }

    public function testParseTemporaryTableLowercase(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('create temporary table t (id integer)');
        self::assertNotNull($def);
        self::assertSame(['id'], $def->columns);
    }

    public function testParseTempTableLowercase(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('create temp table t (id integer)');
        self::assertNotNull($def);
        self::assertSame(['id'], $def->columns);
    }

    public function testParseUnloggedTableLowercase(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('create unlogged table t (id integer)');
        self::assertNotNull($def);
        self::assertSame(['id'], $def->columns);
    }

    public function testParseIfNotExistsLowercase(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('create table if not exists t (id integer)');
        self::assertNotNull($def);
        self::assertSame(['id'], $def->columns);
    }

    public function testParseColumnWithDoubleQuoteName(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t ("col_name" TEXT)');
        self::assertNotNull($def);
        self::assertSame(['col_name'], $def->columns);
    }

    public function testParseSingleQuoteInDefault(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse("CREATE TABLE t (name TEXT DEFAULT 'it''s', id INTEGER)");
        self::assertNotNull($def);
        self::assertSame(['name', 'id'], $def->columns);
    }

    public function testParseTypedColumnsPresent(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (id INTEGER, name TEXT)');
        self::assertNotNull($def);
        self::assertArrayHasKey('id', $def->typedColumns);
        self::assertArrayHasKey('name', $def->typedColumns);
        self::assertSame(ColumnTypeFamily::INTEGER, $def->typedColumns['id']->family);
        self::assertSame(ColumnTypeFamily::TEXT, $def->typedColumns['name']->family);
    }

    public function testParseColumnWithLeadingWhitespaceInDefinition(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse("CREATE TABLE t (\n  id INTEGER,\n  name TEXT\n)");
        self::assertNotNull($def);
        self::assertSame(['id', 'name'], $def->columns);
    }

    public function testParseColumnDefinitionLowercaseType(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (id integer, name text)');
        self::assertNotNull($def);
        self::assertSame('INTEGER', $def->columnTypes['id']);
        self::assertSame('TEXT', $def->columnTypes['name']);
    }

    public function testParseColumnWithDefaultContainingParenInString(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse("CREATE TABLE t (note TEXT DEFAULT '(value)', id INTEGER)");
        self::assertNotNull($def);
        self::assertSame(['note', 'id'], $def->columns);
    }

    public function testParseMultiWordTypeDoublePrecsion(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (val DOUBLE PRECISION)');
        self::assertNotNull($def);
        self::assertSame('DOUBLE PRECISION', $def->columnTypes['val']);
        self::assertSame(ColumnTypeFamily::DOUBLE, $def->typedColumns['val']->family);
    }

    public function testParseVarcharWithLength(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (name VARCHAR(255))');
        self::assertNotNull($def);
        self::assertSame('VARCHAR(255)', $def->columnTypes['name']);
        self::assertSame(ColumnTypeFamily::STRING, $def->typedColumns['name']->family);
    }

    public function testParseUnknownTypeFamily(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (geom GEOMETRY)');
        self::assertNotNull($def);
        self::assertSame(ColumnTypeFamily::UNKNOWN, $def->typedColumns['geom']->family);
    }

    public function testParseUniqueConstraintReferencesValidColumn(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (id INTEGER, name TEXT, UNIQUE(name))');
        self::assertNotNull($def);
        self::assertNotEmpty($def->uniqueConstraints);
        self::assertContains(['name'], $def->uniqueConstraints);
    }

    public function testParsePrimaryKeyConstraintInColumn(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)');
        self::assertNotNull($def);
        self::assertSame(['id'], $def->primaryKeys);
        self::assertContains('id', $def->notNullColumns);
    }

    public function testParseColumnWithLeadingWhitespaceInType(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse("CREATE TABLE t (\n  id  INTEGER\n)");
        self::assertNotNull($def);
        self::assertSame(['id'], $def->columns);
    }

    public function testParseConstraintLowercaseConstraintKeyword(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (id INTEGER, name TEXT, constraint uq_name UNIQUE(name))');
        self::assertNotNull($def);
        self::assertNotEmpty($def->uniqueConstraints);
        self::assertArrayHasKey('uq_name', $def->uniqueConstraints);
        self::assertSame(['name'], $def->uniqueConstraints['uq_name']);
    }

    public function testParseIsConstraintEntryWithLeadingWhitespace(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse("CREATE TABLE t (\n  id INTEGER,\n  PRIMARY KEY (id)\n)");
        self::assertNotNull($def);
        self::assertSame(['id'], $def->primaryKeys);
    }

    public function testParseColumnDefinitionMultilineEntry(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse("CREATE TABLE t (id\nINTEGER)");
        self::assertNotNull($def);
        self::assertSame(['id'], $def->columns);
    }

    public function testParseColumnArrayTypeBrackets(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (tags TEXT[])');
        self::assertNotNull($def);
        self::assertSame(['tags'], $def->columns);
    }

    public function testParseColumnArrayTypeMultipleBrackets(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (matrix INTEGER[ ][ ])');
        self::assertNotNull($def);
        self::assertSame(['matrix'], $def->columns);
    }

    public function testParseColumnArrayTypeBracketsWithSpaceBeforeNext(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (tags TEXT[] NOT NULL)');
        self::assertNotNull($def);
        self::assertSame(['tags'], $def->columns);
        self::assertContains('tags', $def->notNullColumns);
    }

    public function testParseMultiWordTypeWithSecondWordAsConstraint(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (t1 TIMESTAMP NOT NULL)');
        self::assertNotNull($def);
        self::assertSame(['t1'], $def->columns);
        self::assertContains('t1', $def->notNullColumns);
    }

    public function testParseDoublePrecsionLowercase(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (x double precision)');
        self::assertNotNull($def);
        self::assertSame(['x'], $def->columns);
    }

    public function testParseCharacterVaryingTypeWithDifferentLength(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (name CHARACTER VARYING(100))');
        self::assertNotNull($def);
        self::assertSame(['name'], $def->columns);
    }

    public function testParseColumnDefaultWithCommaInSingleQuotedString(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse("CREATE TABLE t (id INTEGER, name TEXT DEFAULT 'a, b')");
        self::assertNotNull($def);
        self::assertSame(['id', 'name'], $def->columns);
    }

    public function testParseColumnWithDoubleQuotedNameContainingComma(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t ("a,b" INTEGER, c TEXT)');
        self::assertNotNull($def);
        self::assertSame(['a,b', 'c'], $def->columns);
    }

    public function testParseConstraintEntryLowercaseCheckKeyword(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (id INTEGER, check (id > 0))');
        self::assertNotNull($def);
        self::assertSame(['id'], $def->columns);
    }

    public function testParseConstraintEntryLowercaseForeignKey(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (id INTEGER, foreign key (id) REFERENCES other(id))');
        self::assertNotNull($def);
        self::assertSame(['id'], $def->columns);
    }

    public function testParseColumnWithDefaultContainingParenthesis(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse("CREATE TABLE t (id INTEGER DEFAULT (nextval('seq')))");
        self::assertNotNull($def);
        self::assertSame(['id'], $def->columns);
    }

    public function testParseBitVaryingMultiWordType(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (flags BIT VARYING(8))');
        self::assertNotNull($def);
        self::assertSame(['flags'], $def->columns);
    }

    public function testParseIntervalYearMultiWordType(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (duration INTERVAL YEAR)');
        self::assertNotNull($def);
        self::assertSame(['duration'], $def->columns);
    }

    public function testParseLowercaseTableLevelPrimaryKey(): void
    {
        $parser = new PgSqlSchemaParser();
        $def = $parser->parse('CREATE TABLE t (id INTEGER, primary key (id))');
        self::assertNotNull($def);
        self::assertSame(['id'], $def->primaryKeys);
    }
}
