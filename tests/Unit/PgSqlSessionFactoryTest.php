<?php

declare(strict_types=1);

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use ZtdQuery\Config\ZtdConfig;
use ZtdQuery\Connection\ConnectionInterface;
use ZtdQuery\Connection\StatementInterface;
use ZtdQuery\Platform\Postgres\PgSqlSessionFactory;
use ZtdQuery\Session;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;
use ZtdQuery\Platform\Postgres\PgSqlCastRenderer;
use ZtdQuery\Platform\Postgres\PgSqlIdentifierQuoter;
use ZtdQuery\Platform\Postgres\PgSqlMutationResolver;
use ZtdQuery\Platform\Postgres\PgSqlParser;
use ZtdQuery\Platform\Postgres\PgSqlQueryGuard;
use ZtdQuery\Platform\Postgres\PgSqlRewriter;
use ZtdQuery\Platform\Postgres\PgSqlSchemaParser;
use ZtdQuery\Platform\Postgres\PgSqlSchemaReflector;
use ZtdQuery\Platform\Postgres\PgSqlTransformer;
use ZtdQuery\Platform\Postgres\Transformer\DeleteTransformer;
use ZtdQuery\Platform\Postgres\Transformer\InsertTransformer;
use ZtdQuery\Platform\Postgres\Transformer\SelectTransformer;
use ZtdQuery\Platform\Postgres\Transformer\UpdateTransformer;

#[CoversClass(PgSqlSessionFactory::class)]
#[UsesClass(PgSqlParser::class)]
#[UsesClass(PgSqlSchemaParser::class)]
#[UsesClass(PgSqlQueryGuard::class)]
#[UsesClass(PgSqlRewriter::class)]
#[UsesClass(PgSqlMutationResolver::class)]
#[UsesClass(PgSqlTransformer::class)]
#[UsesClass(PgSqlSchemaReflector::class)]
#[UsesClass(PgSqlCastRenderer::class)]
#[UsesClass(PgSqlIdentifierQuoter::class)]
#[UsesClass(SelectTransformer::class)]
#[UsesClass(InsertTransformer::class)]
#[UsesClass(UpdateTransformer::class)]
#[UsesClass(DeleteTransformer::class)]
final class PgSqlSessionFactoryTest extends TestCase
{
    public function testCreateReturnsSession(): void
    {
        $connection = static::createStub(ConnectionInterface::class);

        $tablesStmt = static::createStub(StatementInterface::class);
        $tablesStmt->method('fetchAll')->willReturn([]);

        $connection->method('query')->willReturn($tablesStmt);

        $factory = new PgSqlSessionFactory();
        $session = $factory->create($connection, ZtdConfig::default());

        self::assertInstanceOf(Session::class, $session);
    }

    public function testCreatedSessionIsEnabledByDefault(): void
    {
        $connection = static::createStub(ConnectionInterface::class);

        $tablesStmt = static::createStub(StatementInterface::class);
        $tablesStmt->method('fetchAll')->willReturn([]);

        $connection->method('query')->willReturn($tablesStmt);

        $factory = new PgSqlSessionFactory();
        $session = $factory->create($connection, ZtdConfig::default());

        self::assertTrue($session->isEnabled());
    }

    public function testCreateWithTablesReflectsSchema(): void
    {
        $connection = static::createStub(ConnectionInterface::class);

        $tablesStmt = static::createStub(StatementInterface::class);
        $tablesStmt->method('fetchAll')->willReturn([
            ['table_name' => 'users'],
        ]);

        $columnsStmt = static::createStub(StatementInterface::class);
        $columnsStmt->method('fetchAll')->willReturn([
            [
                'column_name' => 'id',
                'data_type' => 'INTEGER',
                'character_maximum_length' => null,
                'numeric_precision' => 32,
                'numeric_scale' => 0,
                'is_nullable' => 'NO',
                'column_default' => null,
                'udt_name' => 'int4',
            ],
            [
                'column_name' => 'name',
                'data_type' => 'TEXT',
                'character_maximum_length' => null,
                'numeric_precision' => null,
                'numeric_scale' => null,
                'is_nullable' => 'YES',
                'column_default' => null,
                'udt_name' => 'text',
            ],
        ]);

        $pkStmt = static::createStub(StatementInterface::class);
        $pkStmt->method('fetchAll')->willReturn([
            ['column_name' => 'id'],
        ]);

        $uniqueStmt = static::createStub(StatementInterface::class);
        $uniqueStmt->method('fetchAll')->willReturn([]);

        $connection->method('query')->willReturnCallback(
            function (string $sql) use ($tablesStmt, $columnsStmt, $pkStmt, $uniqueStmt) {
                if (str_contains($sql, 'information_schema.tables')) {
                    return $tablesStmt;
                }
                if (str_contains($sql, 'information_schema.columns')) {
                    return $columnsStmt;
                }
                if (str_contains($sql, "constraint_type = 'PRIMARY KEY'")) {
                    return $pkStmt;
                }
                if (str_contains($sql, "constraint_type = 'UNIQUE'")) {
                    return $uniqueStmt;
                }

                return false;
            }
        );

        $factory = new PgSqlSessionFactory();
        $session = $factory->create($connection, ZtdConfig::default());

        self::assertInstanceOf(Session::class, $session);
    }

    public function testCreateWithEmptyDatabaseReturnsSession(): void
    {
        $connection = static::createStub(ConnectionInterface::class);

        $connection->method('query')->willReturn(false);

        $factory = new PgSqlSessionFactory();
        $session = $factory->create($connection, ZtdConfig::default());

        self::assertInstanceOf(Session::class, $session);
    }

    public function testSessionCanBeEnabledAfterCreation(): void
    {
        $connection = static::createStub(ConnectionInterface::class);

        $tablesStmt = static::createStub(StatementInterface::class);
        $tablesStmt->method('fetchAll')->willReturn([]);

        $connection->method('query')->willReturn($tablesStmt);

        $factory = new PgSqlSessionFactory();
        $session = $factory->create($connection, ZtdConfig::default());

        $session->enable();
        self::assertTrue($session->isEnabled());
    }

    public function testCreateWithTableRegistersSchemaInSession(): void
    {
        $connection = static::createStub(ConnectionInterface::class);

        $tablesStmt = static::createStub(StatementInterface::class);
        $tablesStmt->method('fetchAll')->willReturn([
            ['table_name' => 'products'],
        ]);

        $columnsStmt = static::createStub(StatementInterface::class);
        $columnsStmt->method('fetchAll')->willReturn([
            [
                'column_name' => 'id',
                'data_type' => 'INTEGER',
                'character_maximum_length' => null,
                'numeric_precision' => 32,
                'numeric_scale' => 0,
                'is_nullable' => 'NO',
                'column_default' => null,
                'udt_name' => 'int4',
            ],
            [
                'column_name' => 'price',
                'data_type' => 'NUMERIC',
                'character_maximum_length' => null,
                'numeric_precision' => 10,
                'numeric_scale' => 2,
                'is_nullable' => 'YES',
                'column_default' => null,
                'udt_name' => 'numeric',
            ],
        ]);

        $pkStmt = static::createStub(StatementInterface::class);
        $pkStmt->method('fetchAll')->willReturn([
            ['column_name' => 'id'],
        ]);

        $uniqueStmt = static::createStub(StatementInterface::class);
        $uniqueStmt->method('fetchAll')->willReturn([]);

        $connection->method('query')->willReturnCallback(
            function (string $sql) use ($tablesStmt, $columnsStmt, $pkStmt, $uniqueStmt) {
                if (str_contains($sql, 'information_schema.tables')) {
                    return $tablesStmt;
                }
                if (str_contains($sql, 'information_schema.columns')) {
                    return $columnsStmt;
                }
                if (str_contains($sql, "constraint_type = 'PRIMARY KEY'")) {
                    return $pkStmt;
                }
                if (str_contains($sql, "constraint_type = 'UNIQUE'")) {
                    return $uniqueStmt;
                }

                return false;
            }
        );

        $factory = new PgSqlSessionFactory();
        $session = $factory->create($connection, ZtdConfig::default());
        $session->enable();

        $plan = $session->rewrite('SELECT * FROM products');
        self::assertStringContainsString('"products" AS MATERIALIZED', $plan->sql());
    }

    public function testCreateWithNullParseResultStillWorks(): void
    {
        $connection = static::createStub(ConnectionInterface::class);

        $tablesStmt = static::createStub(StatementInterface::class);
        $tablesStmt->method('fetchAll')->willReturn([
            ['table_name' => 'bad_table'],
        ]);

        $columnsStmt = static::createStub(StatementInterface::class);
        $columnsStmt->method('fetchAll')->willReturn([]);

        $connection->method('query')->willReturnCallback(
            function (string $sql) use ($tablesStmt, $columnsStmt) {
                if (str_contains($sql, 'information_schema.tables')) {
                    return $tablesStmt;
                }

                return $columnsStmt;
            }
        );

        $factory = new PgSqlSessionFactory();
        $session = $factory->create($connection, ZtdConfig::default());
        self::assertInstanceOf(Session::class, $session);
    }
}
