<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Schema\Reflection\Key;

use ZtdQuery\Connection\StatementInterface;

/**
 * Renders primary-key columns returned by the PostgreSQL catalog.
 *
 * @visibility root
 */
final class PrimaryColumns
{
    /**
     * Renders the ordered primary-key column list, ignoring malformed metadata rows.
     * @return list<string>
     */
    public function columns(StatementInterface|false $statement): array
    {
        $primaryKeyCols = [];
        if ($statement !== false) {
            $pkRows = $statement->fetchAll();
            foreach ($pkRows as $pkRow) {
                $colName = $pkRow['column_name'] ?? null;
                if (is_string($colName)) {
                    $primaryKeyCols[] = '"' . $colName . '"';
                }
            }
        }

        return $primaryKeyCols;
    }
}
