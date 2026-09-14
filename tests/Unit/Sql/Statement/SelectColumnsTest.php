<?php

declare(strict_types=1);

namespace Tests\Unit\Sql\Statement;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Sql\Statement\SelectColumns::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Lexing\QuotedSpan::class)]
final class SelectColumnsTest extends TestCase
{
    public function testExtractSelectColumnNames(): void
    {
        self::assertSame(['id', 'title'], (new \ZtdQuery\Platform\Postgres\Sql\Statement\SelectColumns())->extractSelectColumnNames('SELECT id, name AS title FROM users'));

        self::assertSame(['one', 'label'], (new \ZtdQuery\Platform\Postgres\Sql\Statement\SelectColumns())->extractSelectColumnNames('SELECT 1 AS one, coalesce(name, \'x\') AS label'));

        self::assertSame([], (new \ZtdQuery\Platform\Postgres\Sql\Statement\SelectColumns())->extractSelectColumnNames('SELECT * FROM users'));
    }

    public function testSplitByTopLevelComma(): void
    {
        self::assertSame(['id', 'coalesce(name, \'a,b\')', '"c,d"'], (new \ZtdQuery\Platform\Postgres\Sql\Statement\SelectColumns())->splitByTopLevelComma('id, coalesce(name, \'a,b\'), "c,d"'));
    }
}
