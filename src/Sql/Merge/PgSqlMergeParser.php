<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Sql\Merge;

use ZtdQuery\Exception\UnsupportedSqlException;
use ZtdQuery\Platform\Postgres\Sql\Cte\HeaderParser;
use ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile;
use ZtdQuery\Sql\SqlTokenStream;

/**
 * Merge parser for PostgreSQL queries.
 */
final class PgSqlMergeParser
{
    private HeaderParser $cteHeader;

    /**
     * Initializes the collaborators and state used by this merge parser.
     */
    public function __construct()
    {
        $this->cteHeader = new HeaderParser();
    }

    /**
     * Parses PostgreSQL SQL into the supported structural representation.
     * @throws UnsupportedSqlException
     */
    public function parse(string $sql): PgSqlMergeStatement
    {
        $statementSql = $this->cteHeader->statementSql($sql);
        $tokens = SqlTokenStream::tokenize($statementSql, PgSqlLexerProfile::create())->significantTokens();
        $parts = new StatementParts();
        $target = $parts->target($sql, $statementSql, $tokens);
        $join = $parts->join($sql, $statementSql, $tokens, $target['using']);
        return new PgSqlMergeStatement(
            $target['name'],
            $target['sql'],
            $target['alias'],
            $join['source'],
            $join['condition'],
            $parts->clauses($sql, $statementSql, $join['when']),
        );
    }
}
