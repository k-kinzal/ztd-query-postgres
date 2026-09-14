<?php

declare(strict_types=1);

namespace Tests\Unit\Sql\Diagnostic;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Sql\Diagnostic\KeywordSearch::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::class)]
final class KeywordSearchTest extends TestCase
{
    public function testContainsKeyword(): void
    {
        $sql = 'EXPLAIN SELECT 1';
        $tokens = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create())->significantTokens();

        self::assertSame(true, \ZtdQuery\Platform\Postgres\Sql\Diagnostic\KeywordSearch::containsKeyword($tokens, ['VACUUM', 'EXPLAIN']));

        $sql = 'EXPLAIN SELECT 1';
        $tokens = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create())->significantTokens();

        self::assertSame(false, \ZtdQuery\Platform\Postgres\Sql\Diagnostic\KeywordSearch::containsKeyword($tokens, ['ANALYZE']));
    }
}
