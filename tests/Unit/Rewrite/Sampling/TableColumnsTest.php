<?php

declare(strict_types=1);

namespace Tests\Unit\Rewrite\Sampling;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Rewrite\Sampling\TableColumns::class)]
final class TableColumnsTest extends TestCase
{
    public function testColumns(): void
    {
        self::assertSame(['id', 'name'], (new \ZtdQuery\Platform\Postgres\Rewrite\Sampling\TableColumns())->columns('users', ['users' => ['columns' => ['id', 'name']]]));
    }
}
