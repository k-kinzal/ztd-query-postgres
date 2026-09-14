<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert;

use ZtdQuery\Exception\UnsupportedSqlException;
use ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile;
use ZtdQuery\Shadow\Mutation\UpsertExpression;
use ZtdQuery\Sql\SqlTokenStream;

/**
 * Upsert expression parser for PostgreSQL queries.
 */
final class PgSqlUpsertExpressionParser
{
    /**
     * Parses PostgreSQL SQL into the supported structural representation.
     * @throws UnsupportedSqlException
     */
    public function parse(string $sql, string $tableName): UpsertExpression
    {
        $cursor = new Expression\ExpressionCursor(
            $sql,
            $tableName,
            SqlTokenStream::tokenize($sql, PgSqlLexerProfile::create())->significantTokens(),
        );
        $expression = (new Expression\PrecedenceParser($cursor))->parseOr();
        if ($cursor->index !== count($cursor->tokens)) {
            throw new UnsupportedSqlException($sql, 'Unsupported UPSERT expression');
        }

        return $expression;
    }

    /**
     * Parses the supported upsert expression subset, returning null on rejection.
     */
    public function parseIfSupported(string $sql, string $tableName): ?UpsertExpression
    {
        try {
            return $this->parse($sql, $tableName);
        } catch (UnsupportedSqlException) {
            return null;
        }
    }
}
