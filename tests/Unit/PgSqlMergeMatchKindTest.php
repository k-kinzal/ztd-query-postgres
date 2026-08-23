<?php

declare(strict_types=1);

namespace Tests\Unit;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Platform\Postgres\PgSqlMergeMatchKind;

#[CoversClass(PgSqlMergeMatchKind::class)]
final class PgSqlMergeMatchKindTest extends TestCase
{
    public function testCasesPreservePostgreSqlKeywords(): void
    {
        self::assertSame('MATCHED', PgSqlMergeMatchKind::Matched->value);
        self::assertSame('NOT MATCHED', PgSqlMergeMatchKind::NotMatched->value);
    }
}
