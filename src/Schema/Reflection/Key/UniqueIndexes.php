<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Schema\Reflection\Key;

use ZtdQuery\Connection\StatementInterface;

/**
 * Reconstructs ordinary and partial unique indexes from catalog results.
 *
 * @visibility root
 */
final class UniqueIndexes
{
    /**
     * Combines every catalog row before rejecting unsupported index definitions.
     */
    public function definitions(StatementInterface|false $statement): IndexDefinitions
    {
        $rows = new IndexRows();
        if ($statement !== false) {
            foreach ($statement->fetchAll() as $row) {
                $rows->append($row);
            }
        }
        return $rows->definitions();
    }
}
