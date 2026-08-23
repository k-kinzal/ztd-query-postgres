<?php

declare(strict_types=1);

namespace Tests\Unit;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Platform\Postgres\PgSqlPartitionPredicateRenderer;
use ZtdQuery\Schema\TablePartitionRelation;

#[CoversClass(PgSqlPartitionPredicateRenderer::class)]
final class PgSqlPartitionPredicateRendererTest extends TestCase
{
    public function testRendersSpecificPartitionPredicate(): void
    {
        $relation = new TablePartitionRelation('events', 'created_at >= DATE \'2024-01-01\'');

        self::assertSame(
            'created_at >= DATE \'2024-01-01\'',
            (new PgSqlPartitionPredicateRenderer())->render($relation, ['FALSE']),
        );
    }

    public function testRendersDefaultPartitionPredicateFromSiblings(): void
    {
        $renderer = new PgSqlPartitionPredicateRenderer();
        $relation = new TablePartitionRelation('events', null);

        self::assertSame(
            'COALESCE(NOT ((year = 2024) OR (year = 2025)), TRUE)',
            $renderer->render($relation, ['year = 2024', 'year = 2025']),
        );
        self::assertSame('TRUE', $renderer->render($relation, []));
    }
}
