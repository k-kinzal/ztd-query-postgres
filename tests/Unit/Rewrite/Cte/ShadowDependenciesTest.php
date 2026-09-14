<?php

declare(strict_types=1);

namespace Tests\Unit\Rewrite\Cte;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Rewrite\Cte\ShadowDependencies::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Cte\HeaderParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Cte\IdentifierReferences::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::class)]
final class ShadowDependenciesTest extends TestCase
{
    public function testRequiredIncludesTransitiveDependenciesAndHonorsDeclaredNames(): void
    {
        $dependencies = new \ZtdQuery\Platform\Postgres\Rewrite\Cte\ShadowDependencies();
        $ctes = ['base' => 'base AS (SELECT 1 AS id)', 'derived' => 'derived AS (SELECT * FROM base)', 'unused' => 'unused AS (SELECT 3)'];
        self::assertSame(['base' => $ctes['base'], 'derived' => $ctes['derived']], $dependencies->required('SELECT * FROM derived', $ctes));
        self::assertSame([], $dependencies->required('WITH base AS (SELECT 9) SELECT * FROM base', $ctes));
    }
}
