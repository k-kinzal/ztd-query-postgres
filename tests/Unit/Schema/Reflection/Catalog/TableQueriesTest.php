<?php

declare(strict_types=1);

namespace Tests\Unit\Schema\Reflection\Catalog;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Schema\Reflection\Catalog\TableQueries::class)]
final class TableQueriesTest extends TestCase
{
    public function testColumnsQueriesTheCurrentSchemaAndEscapesTheTableName(): void
    {
        $statement2 = self::createStub(\ZtdQuery\Connection\StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([['column_name' => 'id']]);
        $queries = [];
        $connection = self::createMock(\ZtdQuery\Connection\ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function (string $sql) use ($statement2, &$queries): \ZtdQuery\Connection\StatementInterface {
            $queries[] = $sql;
            return $statement2;
        });
        $statement = (new \ZtdQuery\Platform\Postgres\Schema\Reflection\Catalog\TableQueries($connection))->columns("O'Brien");
        self::assertNotFalse($statement);
        self::assertSame([['column_name' => 'id']], $statement->fetchAll());
        self::assertCount(1, $queries);
        self::assertStringContainsString('FROM information_schema.columns', $queries[0]);
        self::assertStringContainsString("'O''Brien'", $queries[0]);
        self::assertStringContainsString('current_schema()', $queries[0]);
    }
    public function testPrimaryKeyQueriesTheCurrentSchemaAndEscapesTheTableName(): void
    {
        $statement2 = self::createStub(\ZtdQuery\Connection\StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([['column_name' => 'id']]);
        $queries = [];
        $connection = self::createMock(\ZtdQuery\Connection\ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function (string $sql) use ($statement2, &$queries): \ZtdQuery\Connection\StatementInterface {
            $queries[] = $sql;
            return $statement2;
        });
        $statement = (new \ZtdQuery\Platform\Postgres\Schema\Reflection\Catalog\TableQueries($connection))->primaryKey("O'Brien");
        self::assertNotFalse($statement);
        self::assertSame([['column_name' => 'id']], $statement->fetchAll());
        self::assertCount(1, $queries);
        self::assertStringContainsString('tc.constraint_type = \'PRIMARY KEY\'', $queries[0]);
        self::assertStringContainsString("'O''Brien'", $queries[0]);
        self::assertStringContainsString('current_schema()', $queries[0]);
    }
    public function testUniqueIndexesQueriesTheCurrentSchemaAndEscapesTheTableName(): void
    {
        $statement2 = self::createStub(\ZtdQuery\Connection\StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([['column_name' => 'id']]);
        $queries = [];
        $connection = self::createMock(\ZtdQuery\Connection\ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function (string $sql) use ($statement2, &$queries): \ZtdQuery\Connection\StatementInterface {
            $queries[] = $sql;
            return $statement2;
        });
        $statement = (new \ZtdQuery\Platform\Postgres\Schema\Reflection\Catalog\TableQueries($connection))->uniqueIndexes("O'Brien");
        self::assertNotFalse($statement);
        self::assertSame([['column_name' => 'id']], $statement->fetchAll());
        self::assertCount(1, $queries);
        self::assertStringContainsString('AND NOT index_metadata.indisprimary', $queries[0]);
        self::assertStringContainsString("'O''Brien'", $queries[0]);
        self::assertStringContainsString('current_schema()', $queries[0]);
    }
    public function testForeignKeysQueriesTheCurrentSchemaAndEscapesTheTableName(): void
    {
        $statement2 = self::createStub(\ZtdQuery\Connection\StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([['column_name' => 'id']]);
        $queries = [];
        $connection = self::createMock(\ZtdQuery\Connection\ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function (string $sql) use ($statement2, &$queries): \ZtdQuery\Connection\StatementInterface {
            $queries[] = $sql;
            return $statement2;
        });
        $statement = (new \ZtdQuery\Platform\Postgres\Schema\Reflection\Catalog\TableQueries($connection))->foreignKeys("O'Brien");
        self::assertNotFalse($statement);
        self::assertSame([['column_name' => 'id']], $statement->fetchAll());
        self::assertCount(1, $queries);
        self::assertStringContainsString('pk.ordinal_position = fk.position_in_unique_constraint', $queries[0]);
        self::assertStringContainsString("'O''Brien'", $queries[0]);
        self::assertStringContainsString('current_schema()', $queries[0]);
    }
}
