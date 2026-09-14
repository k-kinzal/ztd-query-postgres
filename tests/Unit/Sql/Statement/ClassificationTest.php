<?php

declare(strict_types=1);

namespace Tests\Unit\Sql\Statement;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Sql\Statement\Classification::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PostgreSqlLexicalMasker::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Lexing\CommentSpan::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Lexing\QuotedSpan::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::class)]
final class ClassificationTest extends TestCase
{
    public function testClassifyWithStatement(): void
    {
        self::assertSame('SELECT', (new \ZtdQuery\Platform\Postgres\Sql\Statement\Classification())->classifyWithStatement('WITH selected AS (SELECT 1) SELECT * FROM selected'));

        self::assertSame(null, (new \ZtdQuery\Platform\Postgres\Sql\Statement\Classification())->classifyWithStatement('WITH broken'));
    }

    public function testClassifySimpleStatement(): void
    {
        self::assertSame('UPDATE', (new \ZtdQuery\Platform\Postgres\Sql\Statement\Classification())->classifySimpleStatement('UPDATE users SET id = 1'));

        self::assertSame(null, (new \ZtdQuery\Platform\Postgres\Sql\Statement\Classification())->classifySimpleStatement('VACUUM users'));
    }
    public function testKeywordAtHonorsWordBoundaries(): void
    {
        $classification = new \ZtdQuery\Platform\Postgres\Sql\Statement\Classification();
        self::assertSame(['keyword' => 'SELECT', 'end' => 6], $classification->keywordAt('SELECT *', 0));
        self::assertNull($classification->keywordAt('xSELECT', 1));
        self::assertSame(['keyword' => null, 'end' => 6], $classification->keywordAt('VACUUM', 0));
    }

    public function testClassifyStatementSkipsCommentsBeforeWith(): void
    {
        $parser = new \ZtdQuery\Platform\Postgres\Sql\Statement\Classification();
        self::assertSame('SELECT', $parser->classifyStatement('/* lead */ WITH selected AS (SELECT 1) SELECT * FROM selected'));
        self::assertSame(null, $parser->classifyStatement('VACUUM users'));
    }
}
