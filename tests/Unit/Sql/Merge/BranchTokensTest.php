<?php

declare(strict_types=1);

namespace Tests\Unit\Sql\Merge;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Sql\Merge\BranchTokens::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::class)]
final class BranchTokensTest extends TestCase
{
    public function testKeywordIndexAfter(): void
    {
        $sql = 'MERGE INTO users u USING incoming s ON u.id = s.id WHEN MATCHED AND s.active THEN UPDATE SET name = s.name WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)';
        $tokens = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create())->significantTokens();

        self::assertSame(4, (new \ZtdQuery\Platform\Postgres\Sql\Merge\BranchTokens())->keywordIndexAfter($tokens, 'USING', $tokens[0]));

        $sql = 'MERGE INTO users u USING incoming s ON u.id = s.id WHEN MATCHED AND s.active THEN UPDATE SET name = s.name WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)';
        $tokens = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create())->significantTokens();

        self::assertSame(null, (new \ZtdQuery\Platform\Postgres\Sql\Merge\BranchTokens())->keywordIndexAfter($tokens, 'ABSENT', $tokens[0]));
    }

    public function testLastKeywordBetween(): void
    {
        $sql = 'MERGE INTO users u USING incoming s ON u.id = s.id WHEN MATCHED AND s.active THEN UPDATE SET name = s.name WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)';
        $tokens = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create())->significantTokens();

        self::assertEquals(new \ZtdQuery\Sql\SqlToken(kind: \ZtdQuery\Sql\SqlTokenKind::Word, text: 'ON', offset: 36, depth: 0, bracketDepth: 0), (new \ZtdQuery\Platform\Postgres\Sql\Merge\BranchTokens())->lastKeywordBetween($tokens, 'ON', $tokens[0], $tokens[count($tokens) - 1]));
    }

    public function testMergeWhenTokens(): void
    {
        $sql = 'MERGE INTO users u USING incoming s ON u.id = s.id WHEN MATCHED AND s.active THEN UPDATE SET name = s.name WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)';
        $tokens = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create())->significantTokens();

        self::assertEquals([new \ZtdQuery\Sql\SqlToken(kind: \ZtdQuery\Sql\SqlTokenKind::Word, text: 'WHEN', offset: 51, depth: 0, bracketDepth: 0), new \ZtdQuery\Sql\SqlToken(kind: \ZtdQuery\Sql\SqlTokenKind::Word, text: 'WHEN', offset: 107, depth: 0, bracketDepth: 0)], (new \ZtdQuery\Platform\Postgres\Sql\Merge\BranchTokens())->mergeWhenTokens($tokens));
    }

    public function testKeywordIndexOutsideCase(): void
    {
        $sql = 'WHEN MATCHED AND CASE WHEN x THEN 1 ELSE 0 END = 1 THEN DELETE';
        $tokens = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create())->significantTokens();

        self::assertSame(13, (new \ZtdQuery\Platform\Postgres\Sql\Merge\BranchTokens())->keywordIndexOutsideCase($tokens, 'THEN'));
    }
}
