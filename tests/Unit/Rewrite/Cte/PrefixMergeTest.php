<?php

declare(strict_types=1);

namespace Tests\Unit\Rewrite\Cte;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Rewrite\Cte\PrefixMerge::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::class)]
final class PrefixMergeTest extends TestCase
{
    public function testPrependRewrittenRetainsRecursiveAndLeadingCommentSyntax(): void
    {
        $original = '/* lead */ WITH RECURSIVE chain AS (SELECT 1) SELECT * FROM chain';
        $offset = strpos($original, 'SELECT *');
        self::assertNotFalse($offset);
        $merged = (new \ZtdQuery\Platform\Postgres\Rewrite\Cte\PrefixMerge())->prependRewritten($original, $offset, 'shadow AS (SELECT 2)', 'SELECT * FROM shadow');
        self::assertSame("/* lead */ WITH RECURSIVE shadow AS (SELECT 2),\nchain AS (SELECT 1)\nSELECT * FROM shadow", $merged);
    }
}
