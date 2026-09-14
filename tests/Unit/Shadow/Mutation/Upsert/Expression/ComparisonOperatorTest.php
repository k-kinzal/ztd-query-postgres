<?php

declare(strict_types=1);

namespace Tests\Unit\Shadow\Mutation\Upsert\Expression;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\ComparisonOperator::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\ExpressionCursor::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\ExpressionLexeme::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::class)]
final class ComparisonOperatorTest extends TestCase
{
    public function testComparisonOperatorConsumesBothCharacters(): void
    {
        $sql = '>= 2';
        $tokens = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create())->significantTokens();
        $cursor = new \ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\ExpressionCursor($sql, 'users', $tokens);
        self::assertSame(\ZtdQuery\Shadow\Mutation\UpsertExpressionKind::GreaterOrEqual, (new \ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\ComparisonOperator($cursor))->comparisonOperator());
        self::assertSame(2, $cursor->index);
    }

    public function testComparisonOperatorLeavesOtherTokensUntouched(): void
    {
        $sql = 'word';
        $tokens = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create())->significantTokens();
        $cursor = new \ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\ExpressionCursor($sql, 'users', $tokens);
        self::assertNull((new \ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\ComparisonOperator($cursor))->comparisonOperator());
        self::assertSame(0, $cursor->index);
    }
}
