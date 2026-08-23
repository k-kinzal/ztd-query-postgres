<?php

declare(strict_types=1);

namespace Fuzz\Robustness\Invariant;

use PHPUnit\Framework\Attributes\CoversNothing;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Shadow\ShadowStore;

#[CoversNothing]
final class ShadowStoreConsistencyCheckerTest extends TestCase
{
    public function testAllowsEmptyInitializedTable(): void
    {
        $store = new ShadowStore();
        $store->ensure('events');

        self::assertNull((new ShadowStoreConsistencyChecker($store))->check('CREATE TABLE events (id INT)'));
    }

    public function testRejectsEmptyTableName(): void
    {
        $store = new ShadowStore();
        $store->ensure('');

        $violation = (new ShadowStoreConsistencyChecker($store))->check('SELECT 1');

        self::assertNotNull($violation);
        self::assertSame('SHADOW_EMPTY_KEY', $violation->id());
    }
}
