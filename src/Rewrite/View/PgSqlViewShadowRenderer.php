<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Rewrite\View;

use ZtdQuery\Platform\Postgres\Sql\Relation\PgSqlSelectRelationParser;
use ZtdQuery\Schema\ViewDefinitionSet;

/**
 * View shadow renderer for PostgreSQL queries.
 */
final class PgSqlViewShadowRenderer
{
    /**
     * @param list<string> $tableNames
     * @return array<string, string>
     */
    public function render(ViewDefinitionSet $views, array $tableNames): array
    {
        $definitions = $views->orderedDefinitions();
        $relationNames = array_merge($tableNames, array_keys($definitions));
        $queries = [];
        $relationParser = new PgSqlSelectRelationParser();
        foreach ($definitions as $viewName => $definition) {
            $queries[$viewName] = $relationParser->unqualify($definition->query, $relationNames);
        }

        return $queries;
    }
}
