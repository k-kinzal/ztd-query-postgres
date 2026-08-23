<?php

declare(strict_types=1);

namespace Tests\Unit;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Exception\UnsupportedSqlException;
use ZtdQuery\Platform\Postgres\PgSqlConflictTarget;
use ZtdQuery\Schema\CandidateKeySet;
use ZtdQuery\Schema\PartialUniqueIndex;

#[CoversClass(PgSqlConflictTarget::class)]
final class PgSqlConflictTargetTest extends TestCase
{
    public function testUnspecifiedTargetKeepsEveryRegularCandidateKey(): void
    {
        $keys = CandidateKeySet::fromSchema(['id'], ['users_email' => ['email']]);

        $resolved = (new PgSqlConflictTarget(false))->resolve($keys, [], 'INSERT');

        self::assertSame($keys, $resolved['keys']);
        self::assertNull($resolved['predicate']);
    }

    public function testRegularUniqueIndexTakesPrecedenceOverPartialIndexInference(): void
    {
        $target = new PgSqlConflictTarget(true, ['email'], "status = 'active'");
        $keys = CandidateKeySet::fromSchema([], ['users_email' => ['email']]);
        $partial = new PartialUniqueIndex('users_active_email', ['email'], "status = 'active'");

        $resolved = $target->resolve($keys, [$partial->name => $partial], 'INSERT');

        self::assertSame(['users_email' => ['email']], $resolved['keys']->keys());
        self::assertNull($resolved['predicate']);
    }

    public function testResolvesPartialIndexWithOrderIndependentColumns(): void
    {
        $target = new PgSqlConflictTarget(true, ['EMAIL', 'TENANT_ID'], "status = 'active'");
        $partial = new PartialUniqueIndex(
            'users_active_email',
            ['tenant_id', 'email'],
            "status = 'active'::text",
        );

        $resolved = $target->resolve(new CandidateKeySet([]), [$partial->name => $partial], 'INSERT');

        self::assertSame(['users_active_email' => ['tenant_id', 'email']], $resolved['keys']->keys());
        self::assertSame("status = 'active'", $resolved['predicate']);
    }

    public function testRejectsPartialIndexWithDifferentColumnArity(): void
    {
        $target = new PgSqlConflictTarget(true, ['email', 'tenant_id'], "status = 'active'");
        $partial = new PartialUniqueIndex('users_active_email', ['email'], "status = 'active'");

        $this->expectException(UnsupportedSqlException::class);

        $target->resolve(new CandidateKeySet([]), [$partial->name => $partial], 'INSERT');
    }

    public function testResolvesNamedConstraintCaseInsensitively(): void
    {
        $keys = CandidateKeySet::fromSchema([], ['Users_Email' => ['email'], 'users_name' => ['name']]);

        $resolved = (new PgSqlConflictTarget(true, constraint: 'users_email'))->resolve($keys, [], 'INSERT');

        self::assertSame(['Users_Email' => ['email']], $resolved['keys']->keys());
    }

    public function testRejectsAmbiguousPartialIndexes(): void
    {
        $target = new PgSqlConflictTarget(true, ['email'], "status = 'active'");
        $active = new PartialUniqueIndex('users_active_email', ['email'], "status = 'active'");
        $pending = new PartialUniqueIndex('users_pending_email', ['email'], "status = 'pending'");

        $this->expectException(UnsupportedSqlException::class);

        $target->resolve(new CandidateKeySet([]), [$active->name => $active, $pending->name => $pending], 'INSERT');
    }
}
