<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Schema\Key;

use ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile;
use ZtdQuery\Schema\Key\ForeignKeyDefinition;
use ZtdQuery\Sql\SqlTokenStream;

/**
 * Foreign key definition parser for PostgreSQL queries.
 */
final class PgSqlForeignKeyDefinitionParser
{
    /**
     * @return array<string, ForeignKeyDefinition>
     */
    public function parseCreateTable(
        string $sql,
    ): array {
        $body = (new DefinitionTokens())->tableBody($sql);
        if ($body === null) {
            return [];
        }

        $foreignKeys = [];
        foreach (SqlTokenStream::tokenize($body, PgSqlLexerProfile::create())->splitTopLevel() as $entry) {
            $stream = SqlTokenStream::tokenize($entry, PgSqlLexerProfile::create());
            $first = $stream->identifierAt();
            $firstKeyword = $stream->firstTopLevelKeyword();
            $inlineColumn = $first !== null && !in_array(
                $firstKeyword,
                ['CONSTRAINT', 'FOREIGN', 'PRIMARY', 'UNIQUE', 'CHECK', 'EXCLUDE'],
                true,
            ) ? $first['name'] : null;
            $name = sprintf('foreign_%d', count($foreignKeys));
            $definition = (new DefinitionEntry())->parseEntry($stream, $name, $inlineColumn);
            if ($definition === null) {
                continue;
            }

            $foreignKeys[$definition['name']] = $definition['foreignKey'];
        }

        return $foreignKeys;
    }
}
