<?php

declare(strict_types=1);

namespace Tests\Unit\Transformer;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Platform\Postgres\Transformer\InsertRowRenderer;

#[CoversClass(InsertRowRenderer::class)]
final class InsertRowRendererTest extends TestCase
{
    public function testParsesPostgresDefaultAndRendersCompleteRow(): void
    {
        $renderer = new InsertRowRenderer();
        $provided = $renderer->providedExpressions(['id', 'name'], ['  default  ', "  'Ada'  "]);

        self::assertSame(['name' => "'Ada'"], $provided);
        self::assertSame([
            'id' => '8',
            'name' => "'Ada'",
            'status' => "'active'",
            'note' => 'NULL',
        ], $renderer->render(
            ['id', 'name', 'status', 'note'],
            $provided,
            ['id' => '99', 'status' => "'active'"],
            ['id' => 8],
        ));
    }

    public function testRejectsMismatchedPostgresValues(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Insert values count does not match column count.');

        (new InsertRowRenderer())->providedExpressions(['id'], []);
    }

    public function testPreservesEveryProvidedPostgresExpression(): void
    {
        self::assertSame(
            ['id' => '42', 'name' => "'Ada'"],
            (new InsertRowRenderer())->providedExpressions(['id', 'name'], ['42', "'Ada'"]),
        );
    }
}
