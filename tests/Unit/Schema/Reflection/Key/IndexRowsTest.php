<?php

declare(strict_types=1);

namespace Tests\Unit\Schema\Reflection\Key;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Schema\Reflection\Key\IndexRows::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Schema\Reflection\Key\IndexDefinitions::class)]
final class IndexRowsTest extends TestCase
{
    public function testAppendAccumulatesCompositeAndPartialIndexes(): void
    {
        $rows = new \ZtdQuery\Platform\Postgres\Schema\Reflection\Key\IndexRows();
        $rows->append(['constraint_name' => 'unique_pair', 'column_name' => 'tenant']);
        $rows->append(['constraint_name' => 'unique_pair', 'column_name' => 'id']);
        $rows->append(['constraint_name' => 'active_email', 'column_name' => 'email', 'predicate' => 'active']);
        $definitions = $rows->definitions();
        self::assertSame(['CONSTRAINT "unique_pair" UNIQUE ("tenant", "id")'], $definitions->sql);
        self::assertSame(['email'], $definitions->partialIndexes['active_email']->columns);
        self::assertSame('active', $definitions->partialIndexes['active_email']->predicate);
    }

    public function testDefinitionsExcludesAnEntireExpressionIndex(): void
    {
        $rows = new \ZtdQuery\Platform\Postgres\Schema\Reflection\Key\IndexRows();
        $rows->append(['constraint_name' => 'expression_index', 'column_name' => 'id']);
        $rows->append(['constraint_name' => 'expression_index', 'column_name' => null]);
        $rows->append(['constraint_name' => '', 'column_name' => 'ignored']);
        self::assertSame([], $rows->definitions()->sql);
        self::assertSame([], $rows->definitions()->partialIndexes);
    }
}
