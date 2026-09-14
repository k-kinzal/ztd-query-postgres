<?php

declare(strict_types=1);

namespace Tests\Unit\Schema\Key;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Schema\Key\DefinitionEntry::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Schema\Key\DefinitionTokens::class)]
final class DefinitionEntryTest extends TestCase
{
    public function testParseEntry(): void
    {
        $sql = 'FOREIGN KEY (parent_id) REFERENCES parents (id) ON DELETE CASCADE';
        $stream = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create());
        $tokens = $stream->significantTokens();

        self::assertEquals(['name' => 'fk_parent', 'foreignKey' => new \ZtdQuery\Schema\Key\ForeignKeyDefinition(columns: ['parent_id'], referencedTable: 'parents', referencedColumns: ['id'], onDelete: \ZtdQuery\Schema\Key\ReferentialAction::Cascade, onUpdate: \ZtdQuery\Schema\Key\ReferentialAction::NoAction)], (new \ZtdQuery\Platform\Postgres\Schema\Key\DefinitionEntry())->parseEntry($stream, 'fk_parent', null));
    }

    public function testForeignKeyColumns(): void
    {
        $sql = 'FOREIGN KEY (parent_id) REFERENCES parents (id) ON DELETE CASCADE';
        $stream = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create());
        $tokens = $stream->significantTokens();

        self::assertSame(['parent_id'], (new \ZtdQuery\Platform\Postgres\Schema\Key\DefinitionEntry())->foreignKeyColumns($stream, $tokens, 5));
    }

    public function testReferencedRelation(): void
    {
        $sql = 'FOREIGN KEY (parent_id) REFERENCES parents (id) ON DELETE CASCADE';
        $stream = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create());
        $tokens = $stream->significantTokens();

        self::assertSame(['table' => 'parents', 'columns' => ['id']], (new \ZtdQuery\Platform\Postgres\Schema\Key\DefinitionEntry())->referencedRelation($stream, $tokens, 6));
    }

    public function testIdentifierList(): void
    {
        $sql = 'FOREIGN KEY (parent_id) REFERENCES parents (id) ON DELETE CASCADE';
        $stream = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create());
        $tokens = $stream->significantTokens();

        self::assertSame(['parent_id'], (new \ZtdQuery\Platform\Postgres\Schema\Key\DefinitionEntry())->identifierList($stream, $tokens, 2));
    }

    public function testAction(): void
    {
        $sql = 'FOREIGN KEY (parent_id) REFERENCES parents (id) ON DELETE CASCADE';
        $stream = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create());
        $tokens = $stream->significantTokens();

        self::assertSame(\ZtdQuery\Schema\Key\ReferentialAction::Cascade, (new \ZtdQuery\Platform\Postgres\Schema\Key\DefinitionEntry())->action($tokens, 'DELETE'));
    }
}
