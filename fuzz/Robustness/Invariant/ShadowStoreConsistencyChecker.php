<?php

declare(strict_types=1);

namespace Fuzz\Robustness\Invariant;

use ZtdQuery\Shadow\ShadowStore;

final class ShadowStoreConsistencyChecker
{
    private ShadowStore $store;

    public function __construct(ShadowStore $store)
    {
        $this->store = $store;
    }

    /**
     * Check that all shadow store tables maintain array-of-arrays structure.
     */
    public function check(string $sql): ?InvariantViolation
    {
        $allData = $this->store->getAll();

        foreach ($allData as $tableName => $rows) {
            if ($rows === []) {
                return new InvariantViolation(
                    'INV-L4-01',
                    sprintf('ShadowStore table "%s" is empty', $tableName),
                    $sql,
                    ['table' => $tableName]
                );
            }

            foreach ($rows as $index => $row) {
                if ($row === []) {
                    return new InvariantViolation(
                        'INV-L4-01',
                        sprintf('ShadowStore table "%s" row %d is empty', $tableName, $index),
                        $sql,
                        ['table' => $tableName, 'row_index' => $index]
                    );
                }
            }
        }

        return null;
    }
}
