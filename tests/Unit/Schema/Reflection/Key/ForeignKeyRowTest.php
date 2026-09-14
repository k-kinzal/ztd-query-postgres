<?php

declare(strict_types=1);

namespace Tests\Unit\Schema\Reflection\Key;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Schema\Reflection\Key\ForeignKeyRow::class)]
final class ForeignKeyRowTest extends TestCase
{
    public function testParseValidatesAllReferencedMetadata(): void
    {
        self::assertSame(['name' => 'fk_user', 'column' => 'user_id', 'table' => 'users', 'referencedColumn' => 'id', 'onUpdate' => 'CASCADE', 'onDelete' => 'RESTRICT'], (new \ZtdQuery\Platform\Postgres\Schema\Reflection\Key\ForeignKeyRow())->parse(['constraint_name' => 'fk_user', 'column_name' => 'user_id', 'foreign_table_name' => 'users', 'foreign_column_name' => 'id', 'update_rule' => 'CASCADE', 'delete_rule' => 'RESTRICT']));
        self::assertNull((new \ZtdQuery\Platform\Postgres\Schema\Reflection\Key\ForeignKeyRow())->parse(['constraint_name' => 'incomplete']));
    }
}
