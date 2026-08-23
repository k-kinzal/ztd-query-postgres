<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres;

enum PgSqlTableSampleMethod: string
{
    case Bernoulli = 'BERNOULLI';
    case System = 'SYSTEM';
}
