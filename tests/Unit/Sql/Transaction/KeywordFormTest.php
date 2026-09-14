<?php

declare(strict_types=1);

namespace Tests\Unit\Sql\Transaction;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Sql\Transaction\KeywordForm::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::class)]
final class KeywordFormTest extends TestCase
{
    public function testMatchesAny(): void
    {
        $sql = 'START TRANSACTION';
        $stream = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create());
        $tokens = $stream->significantTokens();

        self::assertSame(true, (new \ZtdQuery\Platform\Postgres\Sql\Transaction\KeywordForm())->matchesAny($tokens, [['BEGIN'], ['START', 'TRANSACTION']]));
    }

    public function testNameAfter(): void
    {
        $sql = 'SAVEPOINT "SaveHere"';
        $stream = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create());
        $tokens = $stream->significantTokens();

        self::assertSame('SaveHere', (new \ZtdQuery\Platform\Postgres\Sql\Transaction\KeywordForm())->nameAfter($tokens, [['SAVEPOINT']]));
    }

    public function testMatches(): void
    {
        $sql = 'ROLLBACK WORK';
        $stream = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create());
        $tokens = $stream->significantTokens();

        self::assertSame(true, (new \ZtdQuery\Platform\Postgres\Sql\Transaction\KeywordForm())->matches($tokens, ['ROLLBACK', 'WORK']));
    }

    public function testUnquote(): void
    {
        self::assertSame('SaveHere', (new \ZtdQuery\Platform\Postgres\Sql\Transaction\KeywordForm())->unquote('"SaveHere"'));
    }
}
