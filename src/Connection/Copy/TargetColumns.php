<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Connection\Copy;

use ValueError;
use ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile;
use ZtdQuery\Schema\TableDefinition;
use ZtdQuery\Sql\SqlToken;
use ZtdQuery\Sql\SqlTokenKind;
use ZtdQuery\Sql\SqlTokenStream;

/**
 * Target columns operations for PostgreSQL copy.
 *
 * @visibility root
 */
final class TargetColumns
{
    /**
     * @return non-empty-list<string>
     * @throws ValueError
     */
    public function relationParts(string $tableName): array
    {
        $parts = SqlTokenStream::tokenize($tableName, PgSqlLexerProfile::create())->splitTopLevel('.');
        if ($parts === []) {
            throw new ValueError('PostgreSQL COPY table name must not be empty.');
        }
        if (in_array('', $parts, true)) {
            throw new ValueError('PostgreSQL COPY table name must not contain an empty qualifier component.');
        }
        if (count($parts) > 2) {
            throw new ValueError('PostgreSQL COPY table name may contain at most a schema and table component.');
        }

        $components = [];
        foreach ($parts as $part) {
            $tokens = SqlTokenStream::tokenize($part, PgSqlLexerProfile::create())->significantTokens();
            if (count($tokens) !== 1) {
                throw new ValueError('PostgreSQL COPY table name must be an identifier or schema-qualified identifier.');
            }
            $components[] = $this->identifier($tokens[0], 'table name');
        }

        return $components;
    }

    /**
     * @return list<string>
     * @throws ValueError
     */
    public function columns(?string $fields, TableDefinition $definition): array
    {
        if ($fields === null) {
            $columns = [];
            foreach ($definition->columns as $column) {
                if (!isset($definition->generatedExpressions[$column])) {
                    $columns[] = $column;
                }
            }

            return $columns;
        }

        $parts = SqlTokenStream::tokenize($fields, PgSqlLexerProfile::create())->splitTopLevel();
        if ($parts === [] || in_array('', $parts, true)) {
            throw new ValueError('PostgreSQL COPY fields must contain at least one column identifier.');
        }

        $columns = [];
        foreach ($parts as $part) {
            $tokens = SqlTokenStream::tokenize($part, PgSqlLexerProfile::create())->significantTokens();
            if (count($tokens) !== 1) {
                throw new ValueError('Each PostgreSQL COPY field must be a single column identifier.');
            }
            $column = $this->identifier($tokens[0], 'field');
            if (in_array($column, $columns, true)) {
                throw new ValueError(sprintf('PostgreSQL COPY field "%s" is specified more than once.', $column));
            }
            $columns[] = $column;
        }

        return $columns;
    }

    /**
     * Identifier.
     * @throws ValueError
     */
    public function identifier(SqlToken $token, string $subject): string
    {
        $parsed = SqlTokenStream::tokenize($token->text, PgSqlLexerProfile::create())->identifierAt();
        if ($parsed === null) {
            throw new ValueError(sprintf('PostgreSQL COPY %s must be a valid identifier.', $subject));
        }

        return $token->kind === SqlTokenKind::Word ? strtolower($parsed['name']) : $parsed['name'];
    }
}
