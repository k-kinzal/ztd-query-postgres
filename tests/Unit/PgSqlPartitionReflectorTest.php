<?php

declare(strict_types=1);

namespace Tests\Unit;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use Tests\Fake\FakeSequentialConnection;
use Tests\Fake\FakeStatement;
use ZtdQuery\Connection\ConnectionInterface;
use ZtdQuery\Platform\Postgres\PgSqlPartitionParser;
use ZtdQuery\Platform\Postgres\PgSqlPartitionReflector;
use ZtdQuery\Schema\TablePartitionStrategy;

#[CoversClass(PgSqlPartitionReflector::class)]
#[UsesClass(PgSqlPartitionParser::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlLexerProfile::class)]
final class PgSqlPartitionReflectorTest extends TestCase
{
    public function testReflectsKeysAndParentRelationsFromCatalog(): void
    {
        $keyStatement = new FakeStatement([
            ['table_name' => null, 'partition_key' => 'RANGE (id)'],
            ['table_name' => '', 'partition_key' => 'RANGE (id)'],
            ['table_name' => 'broken', 'partition_key' => null],
            ['table_name' => 'invalid', 'partition_key' => 'invalid'],
            ['table_name' => 'logs', 'partition_key' => 'RANGE (log_date)'],
        ]);
        $relationStatement = new FakeStatement([
            ['child_table' => null, 'parent_table' => 'logs', 'predicate' => 'TRUE'],
            ['child_table' => '', 'parent_table' => 'logs', 'predicate' => 'TRUE'],
            ['child_table' => 'missing_parent', 'parent_table' => null, 'predicate' => 'TRUE'],
            ['child_table' => 'empty_parent', 'parent_table' => '', 'predicate' => 'TRUE'],
            ['child_table' => 'missing_predicate', 'parent_table' => 'logs', 'predicate' => null],
            ['child_table' => 'blank_predicate', 'parent_table' => 'logs', 'predicate' => ' '],
            [
                'child_table' => 'logs_2024',
                'parent_table' => 'logs',
                'predicate' => "((log_date >= '2024-01-01'::date) AND (log_date < '2025-01-01'::date))",
            ],
        ]);
        $queries = [];
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(
            static function (string $sql) use (&$queries, $keyStatement, $relationStatement) {
                $queries[] = $sql;

                return count($queries) === 1 ? $keyStatement : $relationStatement;
            },
        );
        $reflector = new PgSqlPartitionReflector($connection);

        $metadata = $reflector->reflect();

        self::assertSame(['logs'], array_keys($metadata['keys']));
        self::assertSame(['logs_2024'], array_keys($metadata['relations']));
        self::assertSame(TablePartitionStrategy::Range, $metadata['keys']['logs']->strategy);
        self::assertSame(['log_date'], $metadata['keys']['logs']->expressions);
        self::assertSame('logs', $metadata['relations']['logs_2024']->parentTable);
        $predicate = $metadata['relations']['logs_2024']->predicate;
        self::assertNotNull($predicate);
        self::assertStringContainsString('log_date >=', $predicate);
        self::assertSame([
            'SELECT c.relname AS table_name, pg_get_partkeydef(c.oid) AS partition_key '
            . 'FROM pg_partitioned_table pt '
            . 'JOIN pg_class c ON c.oid = pt.partrelid '
            . 'JOIN pg_namespace n ON n.oid = c.relnamespace '
            . 'WHERE n.nspname = current_schema() ORDER BY c.relname',
            'SELECT child.relname AS child_table, parent.relname AS parent_table, '
            . 'pg_get_partition_constraintdef(child.oid) AS predicate '
            . 'FROM pg_inherits i '
            . 'JOIN pg_class child ON child.oid = i.inhrelid '
            . 'JOIN pg_namespace child_ns ON child_ns.oid = child.relnamespace '
            . 'JOIN pg_class parent ON parent.oid = i.inhparent '
            . 'JOIN pg_partitioned_table pt ON pt.partrelid = parent.oid '
            . 'WHERE child_ns.nspname = current_schema() ORDER BY child.relname',
        ], $queries);
    }

    public function testReturnsEmptyMetadataWhenCatalogQueriesFail(): void
    {
        $metadata = (new PgSqlPartitionReflector(new FakeSequentialConnection([])))->reflect();

        self::assertSame(['keys' => [], 'relations' => []], $metadata);
    }
}
