<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Rewrite\Cte;

use ZtdQuery\Platform\Postgres\Sql\Cte\HeaderParser;
use ZtdQuery\Platform\Postgres\Sql\Cte\IdentifierReferences;

/**
 * Selects shadow CTEs without colliding with user-declared CTE names.
 *
 * @visibility root
 */
final class ShadowDependencies
{
    /**
     * Includes referenced shadow CTEs and their dependencies in declaration order.
     * @param array<string, string> $tableCtes
     * @return array<string, string>
     */
    public function required(string $sql, array $tableCtes): array
    {
        $declared = array_fill_keys((new HeaderParser())->parseHeader($sql)['names'], true);
        $requiredSql = [$sql];
        $requiredCtes = [];
        foreach (array_reverse($tableCtes, true) as $table => $cte) {
            $normalized = strtolower($table);
            if (isset($declared[$normalized])) {
                continue;
            }
            $referenced = false;
            foreach ($requiredSql as $requiredPart) {
                if ((new IdentifierReferences())->referencesIdentifier($requiredPart, $table)) {
                    $referenced = true;
                }
            }
            if (!$referenced) {
                continue;
            }
            $requiredCtes[$table] = $cte;
            $requiredSql[] = $cte;
        }

        $requiredCtes = array_reverse($requiredCtes, true);
        return $requiredCtes;
    }
}
