<?php

declare(strict_types=1);

namespace Tests\Unit\Schema\Reflection\Key;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Schema\Reflection\Key\UniqueIndexes::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Schema\Reflection\Key\IndexDefinitions::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Schema\Reflection\Key\IndexRows::class)]
final class UniqueIndexesTest extends TestCase
{
    public function testDefinitionsReconstructsThePartialIndexPredicate(): void
    {
        $statement = self::createStub(\ZtdQuery\Connection\StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['constraint_name' => 'active_email', 'column_name' => 'email', 'predicate' => "status = 'active'"]]);
        $indexes = (new \ZtdQuery\Platform\Postgres\Schema\Reflection\Key\UniqueIndexes())->definitions($statement);
        self::assertSame([], $indexes->sql);
        self::assertSame("status = 'active'", $indexes->partialIndexes['active_email']->predicate);
        self::assertSame([], (new \ZtdQuery\Platform\Postgres\Schema\Reflection\Key\UniqueIndexes())->definitions(false)->partialIndexes);
    }
}
