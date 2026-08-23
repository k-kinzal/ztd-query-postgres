<?php

declare(strict_types=1);

namespace Tests\Unit;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Platform\Postgres\PgSqlMergeActionKind;
use ZtdQuery\Platform\Postgres\PgSqlMergeClause;
use ZtdQuery\Platform\Postgres\PgSqlMergeMatchKind;

#[CoversClass(PgSqlMergeClause::class)]
#[UsesClass(PgSqlMergeActionKind::class)]
#[UsesClass(PgSqlMergeMatchKind::class)]
final class PgSqlMergeClauseTest extends TestCase
{
    public function testCarriesTypedClauseData(): void
    {
        $clause = new PgSqlMergeClause(
            PgSqlMergeMatchKind::Matched,
            'source.enabled',
            PgSqlMergeActionKind::Update,
            ['name' => 'source.name'],
        );

        self::assertSame(PgSqlMergeMatchKind::Matched, $clause->matchKind);
        self::assertSame('source.enabled', $clause->conditionSql);
        self::assertSame(PgSqlMergeActionKind::Update, $clause->actionKind);
        self::assertSame(['name' => 'source.name'], $clause->assignments);
        self::assertSame([], $clause->insertColumns);
        self::assertSame([], $clause->insertValues);
    }
}
