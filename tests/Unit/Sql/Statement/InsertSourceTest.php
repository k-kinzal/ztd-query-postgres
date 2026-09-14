<?php

declare(strict_types=1);

namespace Tests\Unit\Sql\Statement;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Sql\Statement\InsertSource::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PostgreSqlLexicalMasker::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Lexing\CommentSpan::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Lexing\QuotedSpan::class)]
final class InsertSourceTest extends TestCase
{
    public function testFindInsertSourceClause(): void
    {
        self::assertSame(['keyword' => 'VALUES', 'offset' => 29], (new \ZtdQuery\Platform\Postgres\Sql\Statement\InsertSource())->findInsertSourceClause("INSERT INTO users (id, name) VALUES (1, 'a,b'), (2, 'c')"));
    }

    public function testExtractInsertValues(): void
    {
        self::assertSame([['1', '\'a,b\''], ['2', '\'c\'']], (new \ZtdQuery\Platform\Postgres\Sql\Statement\InsertSource())->extractInsertValues("INSERT INTO users (id, name) VALUES (1, 'a,b'), (2, 'c')"));
    }

    public function testExtractParenthesizedList(): void
    {
        self::assertSame(['items' => ['1', 'coalesce(2, 3)', '\'a,b\''], 'end' => 26], (new \ZtdQuery\Platform\Postgres\Sql\Statement\InsertSource())->extractParenthesizedList("(1, coalesce(2, 3), 'a,b') trailing", 0));
    }

    public function testHasInsertSelect(): void
    {
        self::assertSame(true, (new \ZtdQuery\Platform\Postgres\Sql\Statement\InsertSource())->hasInsertSelect('INSERT INTO users SELECT id FROM source'));
    }

    public function testExtractInsertSelectSql(): void
    {
        self::assertSame('SELECT id FROM source', (new \ZtdQuery\Platform\Postgres\Sql\Statement\InsertSource())->extractInsertSelectSql('INSERT INTO users SELECT id FROM source'));
    }
    public function testAppendListCharacterTreatsArrayCommasAsNested(): void
    {
        $source = new \ZtdQuery\Platform\Postgres\Sql\Statement\InsertSource();
        $depth = 0;
        $items = [];
        $current = 'a';
        $source->appendListCharacter('[', 1, $depth, $items, $current);
        $source->appendListCharacter(',', 1, $depth, $items, $current);
        $source->appendListCharacter(']', 1, $depth, $items, $current);
        self::assertSame([], $items);
        $source->appendListCharacter(',', 1, $depth, $items, $current);
        self::assertSame(['a[,]'], $items);
        self::assertSame('', $current);
        self::assertSame(0, $depth);
    }
}
