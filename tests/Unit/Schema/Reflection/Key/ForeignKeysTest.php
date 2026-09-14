<?php

declare(strict_types=1);

namespace Tests\Unit\Schema\Reflection\Key;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Schema\Reflection\Key\ForeignKeys::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Schema\Reflection\Key\ForeignKeyRow::class)]
final class ForeignKeysTest extends TestCase
{
    public function testDefinitionsGroupsCompositeKeysInCatalogOrder(): void
    {
        $first = ['constraint_name' => 'fk_user', 'column_name' => 'user_id', 'foreign_table_name' => 'users', 'foreign_column_name' => 'id', 'update_rule' => 'CASCADE', 'delete_rule' => 'RESTRICT'];
        $second = array_replace($first, ['column_name' => 'tenant', 'foreign_column_name' => 'tenant_id']);
        $statement = self::createStub(\ZtdQuery\Connection\StatementInterface::class);
        $statement->method('fetchAll')->willReturn([$first, ['constraint_name' => 'invalid'], $second]);
        self::assertSame(['CONSTRAINT "fk_user" FOREIGN KEY ("user_id", "tenant") REFERENCES "users" ("id", "tenant_id") ON UPDATE CASCADE ON DELETE RESTRICT'], (new \ZtdQuery\Platform\Postgres\Schema\Reflection\Key\ForeignKeys())->definitions($statement));
        self::assertSame([], (new \ZtdQuery\Platform\Postgres\Schema\Reflection\Key\ForeignKeys())->definitions(false));
    }
    public function testRenderPreservesReferentialActions(): void
    {
        self::assertSame('CONSTRAINT "fk" FOREIGN KEY ("user_id") REFERENCES "users" ("id") ON UPDATE NO ACTION ON DELETE CASCADE', (new \ZtdQuery\Platform\Postgres\Schema\Reflection\Key\ForeignKeys())->render('fk', ['columns' => ['"user_id"'], 'table' => 'users', 'referencedColumns' => ['"id"'], 'onUpdate' => 'NO ACTION', 'onDelete' => 'CASCADE']));
    }
}
