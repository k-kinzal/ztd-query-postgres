<?php

declare(strict_types=1);

namespace Tests\Unit\Rewrite\Partition;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Rewrite\Partition\StorageTable::class)]
final class StorageTableTest extends TestCase
{
    public function testStorageTableFollowsNestedParentsAndStopsCycles(): void
    {
        $registry = new \ZtdQuery\Schema\TableDefinitionRegistry();
        $base = new \ZtdQuery\Schema\TableDefinition(['id'], ['id' => 'INTEGER'], ['id'], [], []);
        $registry->register('parent', $base);
        $registry->register('child', $base->withPartitionRelation(new \ZtdQuery\Schema\Partition\TablePartitionRelation('parent', 'id < 10')));
        $registry->register('grandchild', $base->withPartitionRelation(new \ZtdQuery\Schema\Partition\TablePartitionRelation('child', 'id < 5')));
        $resolver = new \ZtdQuery\Platform\Postgres\Rewrite\Partition\StorageTable($registry);
        self::assertSame('parent', $resolver->storageTable('grandchild'));
        self::assertSame('unknown', $resolver->storageTable('unknown'));
        $registry->register('parent', $base->withPartitionRelation(new \ZtdQuery\Schema\Partition\TablePartitionRelation('child', 'id < 20')));
        self::assertSame('child', $resolver->storageTable('child'));
    }
}
