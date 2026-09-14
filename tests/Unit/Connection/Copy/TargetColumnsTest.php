<?php

declare(strict_types=1);

namespace Tests\Unit\Connection\Copy;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Connection\Copy\TargetColumns::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::class)]
final class TargetColumnsTest extends TestCase
{
    public function testRelationParts(): void
    {
        self::assertSame(['public', 'Users'], (new \ZtdQuery\Platform\Postgres\Connection\Copy\TargetColumns())->relationParts('public."Users"'));

        self::assertSame(['users'], (new \ZtdQuery\Platform\Postgres\Connection\Copy\TargetColumns())->relationParts('users'));
    }

    public function testColumns(): void
    {
        self::assertSame(['id', 'name'], (new \ZtdQuery\Platform\Postgres\Connection\Copy\TargetColumns())->columns(null, new \ZtdQuery\Schema\TableDefinition(['id', 'name'], ['id' => 'INTEGER', 'name' => 'TEXT'], ['id'], [], [])));

        self::assertSame(['name', 'id'], (new \ZtdQuery\Platform\Postgres\Connection\Copy\TargetColumns())->columns('name,id', new \ZtdQuery\Schema\TableDefinition(['id', 'name'], ['id' => 'INTEGER', 'name' => 'TEXT'], ['id'], [], [])));
    }

    public function testIdentifier(): void
    {
        $sql = '"a""b"';
        $tokens = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create())->significantTokens();

        self::assertSame('a"b', (new \ZtdQuery\Platform\Postgres\Connection\Copy\TargetColumns())->identifier($tokens[0], 'relation'));
    }
}
