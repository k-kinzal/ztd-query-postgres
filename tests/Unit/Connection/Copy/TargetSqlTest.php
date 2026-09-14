<?php

declare(strict_types=1);

namespace Tests\Unit\Connection\Copy;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Connection\Copy\TargetSql::class)]
final class TargetSqlTest extends TestCase
{
    public function testColumnListSql(): void
    {
        self::assertSame('"id", "odd""name"', (new \ZtdQuery\Platform\Postgres\Connection\Copy\TargetSql())->columnListSql(['id', 'odd"name']));
    }

    public function testRelationSql(): void
    {
        self::assertSame('"public"."Users"', (new \ZtdQuery\Platform\Postgres\Connection\Copy\TargetSql())->relationSql(new \ZtdQuery\Platform\CopyTarget(['public', 'Users'], ['id'])));
    }

    public function testQuoteIdentifier(): void
    {
        self::assertSame('"odd""name"', (new \ZtdQuery\Platform\Postgres\Connection\Copy\TargetSql())->quoteIdentifier('odd"name'));
    }
}
