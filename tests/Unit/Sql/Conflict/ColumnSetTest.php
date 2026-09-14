<?php

declare(strict_types=1);

namespace Tests\Unit\Sql\Conflict;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Sql\Conflict\ColumnSet::class)]
final class ColumnSetTest extends TestCase
{
    public function testNormalizedColumns(): void
    {
        self::assertSame(['id', 'name', 'name'], \ZtdQuery\Platform\Postgres\Sql\Conflict\ColumnSet::normalizedColumns(['NAME', 'id', 'Name']));
    }
}
