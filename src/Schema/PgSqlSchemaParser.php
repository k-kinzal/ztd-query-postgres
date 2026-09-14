<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Schema;

use ZtdQuery\Platform\Postgres\Schema\Key\PgSqlForeignKeyDefinitionParser;
use ZtdQuery\Platform\SchemaParser;
use ZtdQuery\Schema\TableDefinition;

/**
 * PostgreSQL implementation of SchemaParser.
 *
 * Parses CREATE TABLE statements into structured schema metadata.
 *
 * @visibility public
 * @example Read primary keys from table DDL
 *     (new \ZtdQuery\Platform\Postgres\Schema\PgSqlSchemaParser())->parse('CREATE TABLE users (id INTEGER PRIMARY KEY)')?->primaryKeys // => ['id']
 */
final class PgSqlSchemaParser implements SchemaParser
{
    /**
     * {@inheritDoc}
     *
     * @visibility public
     * @example Read columns and the primary key from a table definition
     *     $definition = (new \ZtdQuery\Platform\Postgres\Schema\PgSqlSchemaParser())->parse('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
     *     $definition?->columns // => ['id', 'name']
     *     $definition?->primaryKeys // => ['id']
     */
    public function parse(string $createTableSql): ?TableDefinition
    {
        $body = (new Definition\TableBody())->tableBody($createTableSql);
        if ($body === null) {
            return null;
        }

        $fields = new Definition\TableFields();
        $foreignKeys = (new PgSqlForeignKeyDefinitionParser())->parseCreateTable($createTableSql);
        foreach ((new Definition\TableBody())->splitTableBody($body) as $entry) {
            $fields->appendEntry($entry);
        }
        return $fields->definition($createTableSql, $foreignKeys);
    }
}
