<?php

declare(strict_types=1);

namespace Tests\Unit\Sql\Partition;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Sql\Partition\BoundPredicate::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Partition\ClauseTokens::class)]
final class BoundPredicateTest extends TestCase
{
    public function testRangePredicate(): void
    {
        $sql = 'FROM (10) TO (20)';
        $stream = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create());
        $tokens = $stream->significantTokens();
        $key = new \ZtdQuery\Schema\Partition\TablePartitionKey(\ZtdQuery\Schema\Partition\TablePartitionStrategy::Range, ['id']);

        self::assertSame('(id) >= 10 AND (id) < 20', (new \ZtdQuery\Platform\Postgres\Sql\Partition\BoundPredicate())->rangePredicate($sql, $tokens, 0, $key));
    }

    public function testListPredicate(): void
    {
        $sql = 'IN (1, 2, NULL)';
        $stream = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create());
        $tokens = $stream->significantTokens();
        $key = new \ZtdQuery\Schema\Partition\TablePartitionKey(\ZtdQuery\Schema\Partition\TablePartitionStrategy::Range, ['id']);

        self::assertSame('((id) IN (1, 2) OR (id) IS NULL)', (new \ZtdQuery\Platform\Postgres\Sql\Partition\BoundPredicate())->listPredicate($sql, $tokens, 0, $key));
    }

    public function testRangeBoundary(): void
    {
        self::assertSame('(id) >= 10', (new \ZtdQuery\Platform\Postgres\Sql\Partition\BoundPredicate())->rangeBoundary(['id'], ['10'], '>=', 'MINVALUE'));
    }
}
