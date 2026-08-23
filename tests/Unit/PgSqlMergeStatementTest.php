<?php

declare(strict_types=1);

namespace Tests\Unit;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Platform\Postgres\PgSqlMergeActionKind;
use ZtdQuery\Platform\Postgres\PgSqlMergeClause;
use ZtdQuery\Platform\Postgres\PgSqlMergeMatchKind;
use ZtdQuery\Platform\Postgres\PgSqlMergeStatement;

#[CoversClass(PgSqlMergeStatement::class)]
#[UsesClass(PgSqlMergeActionKind::class)]
#[UsesClass(PgSqlMergeClause::class)]
#[UsesClass(PgSqlMergeMatchKind::class)]
final class PgSqlMergeStatementTest extends TestCase
{
    public function testCarriesTypedStatementData(): void
    {
        $clause = new PgSqlMergeClause(
            PgSqlMergeMatchKind::NotMatched,
            null,
            PgSqlMergeActionKind::Insert,
            [],
            ['id'],
            ['source.id'],
        );
        $statement = new PgSqlMergeStatement(
            'users',
            'public.users',
            'target',
            'source AS source',
            'target.id = source.id',
            [$clause],
        );

        self::assertSame('users', $statement->targetTable);
        self::assertSame('public.users', $statement->targetSql);
        self::assertSame('target', $statement->targetAlias);
        self::assertSame('source AS source', $statement->sourceSql);
        self::assertSame('target.id = source.id', $statement->joinConditionSql);
        self::assertSame([$clause], $statement->clauses);
    }
}
