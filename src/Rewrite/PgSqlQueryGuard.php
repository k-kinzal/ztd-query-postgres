<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Rewrite;

use ZtdQuery\Platform\Postgres\Sql\Diagnostic\PgSqlReadOnlyDiagnosticStatement;
use ZtdQuery\Platform\Postgres\Sql\PgSqlParser;
use ZtdQuery\Rewrite\QueryKind;

/**
 * Classifies PostgreSQL SQL statements into QueryKind categories.
 *
 * @visibility public
 * @example Route reads, simulated writes and transactions
 *     $guard = new \ZtdQuery\Platform\Postgres\Rewrite\PgSqlQueryGuard(new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser());
 *     $guard->classify('SELECT 1') === \ZtdQuery\Rewrite\QueryKind::READ // => true
 *     $guard->classify('DELETE FROM users') === \ZtdQuery\Rewrite\QueryKind::WRITE_SIMULATED // => true
 *     $guard->classify('BEGIN') === \ZtdQuery\Rewrite\QueryKind::SKIPPED // => true
 *     $guard->classify('VACUUM') // => null
 */
final class PgSqlQueryGuard
{
    private PgSqlParser $parser;

    /**
     * Initializes the collaborators and state used by this query guard.
     */
    public function __construct(PgSqlParser $parser)
    {
        $this->parser = $parser;
    }

    /**
     * Classify a SQL string into READ/WRITE_SIMULATED/DDL_SIMULATED/SKIPPED or null.
     * @visibility public
     * @example Route reads, simulated writes and transactions
     *     $guard = new \ZtdQuery\Platform\Postgres\Rewrite\PgSqlQueryGuard(new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser());
     *     $guard->classify('SELECT 1') === \ZtdQuery\Rewrite\QueryKind::READ // => true
     *     $guard->classify('DELETE FROM users') === \ZtdQuery\Rewrite\QueryKind::WRITE_SIMULATED // => true
     *     $guard->classify('BEGIN') === \ZtdQuery\Rewrite\QueryKind::SKIPPED // => true
     *     $guard->classify('VACUUM') // => null
     */
    public function classify(string $sql): ?QueryKind
    {
        if (PgSqlReadOnlyDiagnosticStatement::isSafe($sql)) {
            return QueryKind::READ;
        }
        $type = $this->parser->classifyStatement($sql);
        if ($type === null) {
            return null;
        }

        return match ($type) {
            'SELECT' => QueryKind::READ,
            'DO' => QueryKind::READ,
            'INSERT', 'UPDATE', 'DELETE', 'MERGE', 'TRUNCATE' => QueryKind::WRITE_SIMULATED,
            'CREATE_TABLE', 'DROP_TABLE', 'ALTER_TABLE' => QueryKind::DDL_SIMULATED,
            'TCL' => QueryKind::SKIPPED,
        };
    }
}
