<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Schema;

use ZtdQuery\Connection\ConnectionInterface;
use ZtdQuery\Platform\Postgres\Schema\View\PgSqlViewDefinitionParser;
use ZtdQuery\Platform\SchemaReflector;
use ZtdQuery\Platform\ViewReflector;
use ZtdQuery\Schema\Key\PartialUniqueIndex;

/**
 * Fetches PostgreSQL schema information via information_schema queries.
 *
 * Reconstructs CREATE TABLE statements from pg_catalog/information_schema
 * since PostgreSQL has no SHOW CREATE TABLE equivalent.
 */
final class PgSqlSchemaReflector implements SchemaReflector, ViewReflector
{
    private ConnectionInterface $connection;

    /**
     * @var array<string, array<string, PartialUniqueIndex>>
     */
    private array $partialUniqueIndexes = [];

    /**
     * Initializes the collaborators and state used by this schema reflector.
     */
    public function __construct(ConnectionInterface $connection)
    {
        $this->connection = $connection;
    }

    /**
     * {@inheritDoc}
     */
    public function getCreateStatement(string $tableName): ?string
    {
        $queries = new Reflection\Catalog\TableQueries($this->connection);
        $statement = $queries->columns($tableName);
        if ($statement === false) {
            return null;
        }
        $columns = $statement->fetchAll();
        if ($columns === []) {
            return null;
        }
        $parts = [];
        foreach ($columns as $column) {
            $parts[] = (new Reflection\Column\ColumnDefinitionSql())->buildColumnDefinition($column);
        }
        $primaryKey = (new Reflection\Key\PrimaryColumns())->columns($queries->primaryKey($tableName));
        $unique = (new Reflection\Key\UniqueIndexes())->definitions($queries->uniqueIndexes($tableName));
        $this->partialUniqueIndexes[$tableName] = $unique->partialIndexes;
        $foreignKeys = (new Reflection\Key\ForeignKeys())->definitions($queries->foreignKeys($tableName));
        if ($primaryKey !== []) {
            $parts[] = 'PRIMARY KEY (' . implode(', ', $primaryKey) . ')';
        }
        $parts = array_merge($parts, $unique->sql, $foreignKeys);
        return 'CREATE TABLE "' . $tableName . '" (' . "\n  " . implode(",\n  ", $parts) . "\n)";
    }

    /**
     * {@inheritDoc}
     */
    public function reflectAll(): array
    {
        $stmt = $this->connection->query(
            'SELECT table_name FROM information_schema.tables '
            . "WHERE table_schema = current_schema() AND table_type = 'BASE TABLE' "
            . 'ORDER BY table_name'
        );
        if ($stmt === false) {
            return [];
        }

        $tables = $stmt->fetchAll();
        $result = [];

        foreach ($tables as $row) {
            $tableName = $row['table_name'] ?? null;
            if (!is_string($tableName) || $tableName === '') {
                continue;
            }

            $createSql = $this->getCreateStatement($tableName);
            if ($createSql !== null) {
                $result[$tableName] = $createSql;
            }
        }

        return $result;
    }

    /**
     * @return array<string, array<string, PartialUniqueIndex>>
     */
    public function partialUniqueIndexes(): array
    {
        return $this->partialUniqueIndexes;
    }

    /**
     * {@inheritDoc}
     */
    public function reflectViews(): array
    {
        $stmt = $this->connection->query(
            'SELECT viewname, definition FROM pg_views WHERE schemaname = current_schema() ORDER BY viewname',
        );
        if ($stmt === false) {
            return [];
        }

        $definitions = [];
        foreach ($stmt->fetchAll() as $row) {
            $viewName = $row['viewname'] ?? null;
            $query = $row['definition'] ?? null;
            if (!is_string($viewName) || $viewName === '' || !is_string($query) || trim($query) === '') {
                continue;
            }
            $definitions[$viewName] = (new PgSqlViewDefinitionParser())->fromQuery($query);
        }

        return $definitions;
    }
}
