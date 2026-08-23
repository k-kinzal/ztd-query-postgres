<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres;

enum PgSqlMergeMatchKind: string
{
    case Matched = 'MATCHED';
    case NotMatched = 'NOT MATCHED';
}
