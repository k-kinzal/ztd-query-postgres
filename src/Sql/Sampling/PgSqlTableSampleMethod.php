<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Sql\Sampling;

/**
 * Enumerates the PostgreSQL TABLESAMPLE methods supported by shadow queries.
 */
enum PgSqlTableSampleMethod: string
{
    case Bernoulli = 'BERNOULLI';
    case System = 'SYSTEM';
}
