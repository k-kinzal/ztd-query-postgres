<?php

declare(strict_types=1);

namespace Tests\Unit\Schema\Key;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Schema\Key\DefinitionTokens::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::class)]
final class DefinitionTokensTest extends TestCase
{
    public function testTableBody(): void
    {
        self::assertSame('id INT, parent_id INT REFERENCES parents (id)', (new \ZtdQuery\Platform\Postgres\Schema\Key\DefinitionTokens())->tableBody('CREATE TABLE children (id INT, parent_id INT REFERENCES parents (id))'));
    }

    public function testKeywordIndex(): void
    {
        $sql = 'FOREIGN KEY (parent_id) REFERENCES parents (id) ON DELETE CASCADE';
        $stream = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create());
        $tokens = $stream->significantTokens();

        self::assertSame(5, \ZtdQuery\Platform\Postgres\Schema\Key\DefinitionTokens::keywordIndex($tokens, 'REFERENCES'));
    }

    public function testSymbolIndex(): void
    {
        $sql = 'FOREIGN KEY (parent_id) REFERENCES parents (id) ON DELETE CASCADE';
        $stream = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create());
        $tokens = $stream->significantTokens();

        self::assertSame(2, \ZtdQuery\Platform\Postgres\Schema\Key\DefinitionTokens::symbolIndex($tokens, '(', 0));
    }

    public function testIsSymbol(): void
    {
        self::assertSame(false, \ZtdQuery\Platform\Postgres\Schema\Key\DefinitionTokens::isSymbol(null, '('));
    }
}
