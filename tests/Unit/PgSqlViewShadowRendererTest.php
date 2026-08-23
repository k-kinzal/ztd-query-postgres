<?php

declare(strict_types=1);

namespace Tests\Unit;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Platform\Postgres\PgSqlSelectRelationParser;
use ZtdQuery\Platform\Postgres\PgSqlViewShadowRenderer;
use ZtdQuery\Schema\ViewDefinition;
use ZtdQuery\Schema\ViewDefinitionSet;

#[CoversClass(PgSqlViewShadowRenderer::class)]
#[UsesClass(PgSqlSelectRelationParser::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlLexerProfile::class)]
final class PgSqlViewShadowRendererTest extends TestCase
{
    public function testOrdersViewsAndUnqualifiesShadowedPostgreSqlRelations(): void
    {
        $views = new ViewDefinitionSet();
        $views->register('summary', new ViewDefinition('SELECT count(*) FROM public."active_users"', ['active_users']));
        $views->register('active_users', new ViewDefinition('SELECT * FROM public."users"', ['users']));

        self::assertSame(
            [
                'active_users' => 'SELECT * FROM "users"',
                'summary' => 'SELECT count(*) FROM "active_users"',
            ],
            (new PgSqlViewShadowRenderer())->render($views, ['users']),
        );
    }
}
