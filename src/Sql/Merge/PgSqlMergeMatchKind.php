<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Sql\Merge;

/**
 * Enumerates the target and source matching modes of a MERGE branch.
 */
enum PgSqlMergeMatchKind: string
{
    case Matched = 'MATCHED';
    case NotMatched = 'NOT MATCHED';
}
