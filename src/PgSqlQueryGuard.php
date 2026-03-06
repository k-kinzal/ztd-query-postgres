<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres;

use ZtdQuery\Rewrite\QueryKind;

/**
 * Classifies PostgreSQL SQL statements into QueryKind categories.
 */
final class PgSqlQueryGuard
{
    private PgSqlParser $parser;

    public function __construct(PgSqlParser $parser)
    {
        $this->parser = $parser;
    }

    /**
     * Classify a SQL string into READ/WRITE_SIMULATED/DDL_SIMULATED/SKIPPED or null.
     */
    public function classify(string $sql): ?QueryKind
    {
        $type = $this->parser->classifyStatement($sql);
        if ($type === null) {
            return null;
        }

        return match ($type) {
            'SELECT' => QueryKind::READ,
            'INSERT', 'UPDATE', 'DELETE', 'TRUNCATE' => QueryKind::WRITE_SIMULATED,
            'CREATE_TABLE', 'DROP_TABLE', 'ALTER_TABLE' => QueryKind::DDL_SIMULATED,
            'TCL' => QueryKind::SKIPPED,
            default => null,
        };
    }
}
