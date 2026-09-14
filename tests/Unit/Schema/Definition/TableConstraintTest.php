<?php

declare(strict_types=1);

namespace Tests\Unit\Schema\Definition;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Schema\Definition\TableConstraint::class)]
final class TableConstraintTest extends TestCase
{
    public function testParseConstraint(): void
    {
        $primaryKeys = [];
        $uniqueConstraints = [];
        $uniqueIndex = 0;

        (new \ZtdQuery\Platform\Postgres\Schema\Definition\TableConstraint())->parseConstraint('PRIMARY KEY (id, name)', $primaryKeys, $uniqueConstraints, $uniqueIndex);

        self::assertSame(['id', 'name'], $primaryKeys);

        self::assertSame([], $uniqueConstraints);

        self::assertSame(0, $uniqueIndex);
    }

    public function testIsConstraintEntry(): void
    {
        self::assertSame(true, (new \ZtdQuery\Platform\Postgres\Schema\Definition\TableConstraint())->isConstraintEntry('CONSTRAINT pk PRIMARY KEY (id)'));
    }

    public function testParseColumnRefList(): void
    {
        self::assertSame(['id', 'Name'], (new \ZtdQuery\Platform\Postgres\Schema\Definition\TableConstraint())->parseColumnRefList('id, "Name"'));
    }

    public function testUnquoteIdentifier(): void
    {
        self::assertSame('Users', (new \ZtdQuery\Platform\Postgres\Schema\Definition\TableConstraint())->unquoteIdentifier('"Users"'));
    }
}
