<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Rewrite\Transformer\Update;

use ZtdQuery\Shadow\Mutation\MutationRowIdentity;

/**
 * Builds the SELECT list that represents a PostgreSQL UPDATE result.
 *
 * @visibility root
 */
final class ColumnProjection
{
    /**
     * Projects assigned values, unchanged columns and original row identity metadata.
     * @param array<string, string> $sets
     * @param array<int, string> $columns
     * @param array<int, string> $primaryKeys
     */
    public function render(string $qualifier, array $sets, array $columns, array $primaryKeys): string
    {
        $selectCols = [];
        $coveredCols = [];

        foreach ($sets as $colName => $value) {
            $selectCols[] = $value . ' AS "' . $colName . '"';
            $coveredCols[$colName] = true;
        }

        foreach ($columns as $col) {
            if (!isset($coveredCols[$col])) {
                $selectCols[] = "\"$qualifier\".\"$col\"";
            }
        }

        $identity = new MutationRowIdentity();
        foreach ($primaryKeys as $primaryKey) {
            $selectCols[] = '"' . $qualifier . '"."' . $primaryKey . '" AS "' . $identity->column($primaryKey) . '"';
        }

        if ($selectCols === []) {
            $selectCols[] = '*';
        }

        $selectList = implode(', ', $selectCols);

        return $selectList;
    }
}
