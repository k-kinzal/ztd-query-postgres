<?php

declare(strict_types=1);

namespace Tests\Unit\Rewrite\Transformer\Update;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Rewrite\Transformer\Update\ColumnProjection::class)]
final class ColumnProjectionTest extends TestCase
{
    public function testRenderPreservesUnmodifiedColumnsAndOriginalIdentity(): void
    {
        $projection = new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\Update\ColumnProjection();
        self::assertSame('2 AS "id", "u"."name", "u"."id" AS "__ztd_original_id"', $projection->render('u', ['id' => '2'], ['id', 'name'], ['id']));
        self::assertSame('*', $projection->render('users', [], [], []));
    }
}
