<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Schema\View;

use ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile;
use ZtdQuery\Platform\Postgres\Sql\Relation\PgSqlSelectRelationParser;
use ZtdQuery\Schema\ViewDefinition;
use ZtdQuery\Sql\SqlTokenStream;

/**
 * View definition parser for PostgreSQL queries.
 */
final class PgSqlViewDefinitionParser
{
    /**
     * Builds a view definition from a query and the relations it references.
     */
    public function fromQuery(string $query): ViewDefinition
    {
        $query = rtrim(trim($query), ';');

        return new ViewDefinition($query, (new PgSqlSelectRelationParser())->tableNames($query));
    }

    /**
     * Reads a CREATE VIEW statement and preserves its query definition.
     */
    public function fromCreateStatement(string $sql): ?ViewDefinition
    {
        foreach (SqlTokenStream::tokenize($sql, PgSqlLexerProfile::create())->significantTokens() as $token) {
            if (!$token->isTopLevel() || !$token->isKeyword('AS')) {
                continue;
            }
            $query = substr($sql, $token->endOffset());
            if (trim($query) !== '') {
                return $this->fromQuery($query);
            }
        }

        return null;
    }
}
