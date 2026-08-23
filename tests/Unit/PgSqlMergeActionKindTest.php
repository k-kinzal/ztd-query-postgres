<?php

declare(strict_types=1);

namespace Tests\Unit;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Platform\Postgres\PgSqlMergeActionKind;

#[CoversClass(PgSqlMergeActionKind::class)]
final class PgSqlMergeActionKindTest extends TestCase
{
    public function testCasesPreservePostgreSqlKeywords(): void
    {
        self::assertSame('UPDATE', PgSqlMergeActionKind::Update->value);
        self::assertSame('INSERT', PgSqlMergeActionKind::Insert->value);
        self::assertSame('DELETE', PgSqlMergeActionKind::Delete->value);
        self::assertSame('DO NOTHING', PgSqlMergeActionKind::DoNothing->value);
    }
}
