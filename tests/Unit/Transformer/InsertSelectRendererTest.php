<?php

declare(strict_types=1);

namespace Tests\Unit\Transformer;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Platform\Postgres\PgSqlIdentifierQuoter;
use ZtdQuery\Platform\Postgres\Transformer\InsertSelectRenderer;

#[CoversClass(InsertSelectRenderer::class)]
#[UsesClass(PgSqlIdentifierQuoter::class)]
final class InsertSelectRendererTest extends TestCase
{
    public function testRendersPostgreSqlProjectionFromDialectNeutralPlan(): void
    {
        $sql = (new InsertSelectRenderer())->render(
            'SELECT DISTINCT name, COUNT(*) FROM users GROUP BY name',
            ['id', 'name', 'count', 'status'],
            ['name', 'count'],
            ['status' => "'active'"],
            ['id' => 8],
        );

        self::assertSame(
            'WITH "__ztd_insert_source" ("__ztd_insert_0", "__ztd_insert_1") AS ('
            . 'SELECT DISTINCT name, COUNT(*) FROM users GROUP BY name) '
            . 'SELECT 8 + ROW_NUMBER() OVER () - 1 AS "id", "__ztd_insert_0" AS "name", '
            . '"__ztd_insert_1" AS "count", \'active\' AS "status" FROM "__ztd_insert_source"',
            $sql,
        );
    }

    public function testRendersGeneratedIdentityExpression(): void
    {
        $renderer = new InsertSelectRenderer();

        self::assertSame('1 + ROW_NUMBER() OVER () - 1', $renderer->renderGeneratedIdentity(1));
    }

    public function testRejectsNonPositiveGeneratedIdentityStart(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        (new InsertSelectRenderer())->renderGeneratedIdentity(0);
    }
}
