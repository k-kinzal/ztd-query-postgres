<?php

declare(strict_types=1);

namespace Tests\Unit\Schema\Reflection\Key;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Schema\Reflection\Key\PrimaryColumns::class)]
final class PrimaryColumnsTest extends TestCase
{
    public function testColumnsKeepsCatalogOrderAndSkipsInvalidMetadata(): void
    {
        $statement = self::createStub(\ZtdQuery\Connection\StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['column_name' => 'tenant_id'], ['column_name' => null], ['column_name' => 'id']]);
        self::assertSame(['"tenant_id"', '"id"'], (new \ZtdQuery\Platform\Postgres\Schema\Reflection\Key\PrimaryColumns())->columns($statement));
        self::assertSame([], (new \ZtdQuery\Platform\Postgres\Schema\Reflection\Key\PrimaryColumns())->columns(false));
    }
}
