<?php

declare(strict_types=1);

namespace Tests\Unit;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Platform\Postgres\PgSqlGeneratedColumnProjector;

#[CoversClass(PgSqlGeneratedColumnProjector::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlIdentifierQuoter::class)]
final class PgSqlGeneratedColumnProjectorTest extends TestCase
{
    public function testReturnsSourceUnchangedWithoutGeneratedExpressions(): void
    {
        self::assertSame(
            'SELECT 1 AS "id"',
            (new PgSqlGeneratedColumnProjector())->project('SELECT 1 AS "id"', ['id'], []),
        );
    }

    public function testProjectsGeneratedExpressionsOverBaseRows(): void
    {
        self::assertSame(
            'SELECT "__ztd_generated_source"."qty" AS "qty", '
            . '"__ztd_generated_source"."unit_price" AS "unit_price", '
            . '(qty * unit_price) AS "total" '
            . 'FROM (SELECT 5 AS "qty", 10 AS "unit_price", NULL AS "total") AS "__ztd_generated_source"',
            (new PgSqlGeneratedColumnProjector())->project(
                'SELECT 5 AS "qty", 10 AS "unit_price", NULL AS "total"',
                ['qty', 'unit_price', 'total'],
                ['total' => '(qty * unit_price)'],
            ),
        );
    }
}
