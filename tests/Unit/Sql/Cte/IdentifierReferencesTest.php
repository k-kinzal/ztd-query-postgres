<?php

declare(strict_types=1);

namespace Tests\Unit\Sql\Cte;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Sql\Cte\IdentifierReferences::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Cte\HeaderParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::class)]
final class IdentifierReferencesTest extends TestCase
{
    public function testReferencesIdentifier(): void
    {
        self::assertSame(true, (new \ZtdQuery\Platform\Postgres\Sql\Cte\IdentifierReferences())->referencesIdentifier('SELECT * FROM users', 'users'));
    }

    public function testReferencesAnyIdentifier(): void
    {
        self::assertSame(true, (new \ZtdQuery\Platform\Postgres\Sql\Cte\IdentifierReferences())->referencesAnyIdentifier('SELECT * FROM users', ['orders', 'users']));
    }
}
