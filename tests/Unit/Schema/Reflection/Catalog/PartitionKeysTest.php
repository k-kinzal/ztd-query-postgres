<?php

declare(strict_types=1);

namespace Tests\Unit\Schema\Reflection\Catalog;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Schema\Reflection\Catalog\PartitionKeys::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Partition\PgSqlPartitionParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Partition\BoundPredicate::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Partition\ClauseTokens::class)]
final class PartitionKeysTest extends TestCase
{
    public function testReflectIgnoresMalformedRowsAndKeepsThePartitionExpression(): void
    {
        $statement = self::createStub(\ZtdQuery\Connection\StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['table_name' => 'events', 'partition_key' => 'RANGE (created_at)'], ['table_name' => '', 'partition_key' => 'HASH (id)'], ['table_name' => 'invalid', 'partition_key' => null]]);
        $queries = [];
        $connection = self::createMock(\ZtdQuery\Connection\ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function (string $sql) use ($statement, &$queries): \ZtdQuery\Connection\StatementInterface {
            $queries[] = $sql;
            return $statement;
        });
        $keys = (new \ZtdQuery\Platform\Postgres\Schema\Reflection\Catalog\PartitionKeys($connection))->reflect();
        self::assertSame(['events'], array_keys($keys));
        self::assertSame(\ZtdQuery\Schema\Partition\TablePartitionStrategy::Range, $keys['events']->strategy);
        self::assertSame(['created_at'], $keys['events']->expressions);
        $connection2 = self::createStub(\ZtdQuery\Connection\ConnectionInterface::class);
        $connection2->method('query')->willReturn(false);
        self::assertSame([], (new \ZtdQuery\Platform\Postgres\Schema\Reflection\Catalog\PartitionKeys($connection2))->reflect());
    }
}
