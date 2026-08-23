<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres;

use ZtdQuery\Schema\ViewDefinition;
use ZtdQuery\Sql\SqlTokenStream;

final class PgSqlViewDefinitionParser
{
    public function fromQuery(string $query): ViewDefinition
    {
        $query = rtrim(trim($query), ';');

        return new ViewDefinition($query, (new PgSqlSelectRelationParser())->tableNames($query));
    }

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
