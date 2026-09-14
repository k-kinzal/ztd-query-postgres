<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Shadow\Mutation\Table;

use ZtdQuery\Exception\UnknownSchemaException;
use ZtdQuery\Exception\UnsupportedSqlException;
use ZtdQuery\Platform\Postgres\Sql\Partition\PgSqlPartitionParser;
use ZtdQuery\Platform\Postgres\Sql\PgSqlParser;
use ZtdQuery\Platform\SchemaParser;
use ZtdQuery\Schema\ColumnDeclaration;
use ZtdQuery\Schema\ColumnTypeFamily;
use ZtdQuery\Schema\TableDefinitionRegistry;
use ZtdQuery\Shadow\Mutation\ShadowMutation;
use ZtdQuery\Shadow\Mutation\Table\CreateTableAsSelectMutation;
use ZtdQuery\Shadow\Mutation\Table\CreateTableLikeMutation;
use ZtdQuery\Shadow\Mutation\Table\CreateTableMutation;
use ZtdQuery\Shadow\Mutation\Table\DropTableMutation;

/**
 * Ddl resolver operations for PostgreSQL mutation.
 *
 * @visibility root
 */
final class TableMutationResolver
{
    private readonly PgSqlParser $parser;
    private readonly PgSqlPartitionParser $partitionParser;
    private readonly TableDefinitionRegistry $registry;
    private readonly SchemaParser $schemaParser;

    /**
     * Supplies the dependencies used by this TableMutationResolver.
     */
    public function __construct(PgSqlParser $parser, PgSqlPartitionParser $partitionParser, TableDefinitionRegistry $registry, SchemaParser $schemaParser)
    {
        $this->parser = $parser;
        $this->partitionParser = $partitionParser;
        $this->registry = $registry;
        $this->schemaParser = $schemaParser;
    }

    /**
     * Resolve create table.
     * @throws UnknownSchemaException
     * @throws UnsupportedSqlException
     */
    public function resolveCreateTable(string $sql): ShadowMutation
    {
        $tableName = $this->parser->extractCreateTableName($sql);
        if ($tableName === null) {
            throw new UnsupportedSqlException($sql, 'Cannot resolve table name');
        }

        $ifNotExists = $this->parser->hasIfNotExists($sql);

        if (!$ifNotExists && $this->registry->has($tableName)) {
            throw new UnsupportedSqlException($sql, 'Table already exists');
        }

        $parentTable = $this->partitionParser->parentTable($sql);
        if ($parentTable !== null) {
            return $this->resolvePartition($sql, $tableName, $parentTable, $ifNotExists);
        }

        if ($this->parser->hasCreateTableLike($sql)) {
            $sourceTable = $this->parser->extractCreateTableLikeSource($sql);
            if ($sourceTable === null || !$this->registry->has($sourceTable)) {
                throw new UnknownSchemaException($sql, $sourceTable ?? 'unknown', 'table');
            }

            return new CreateTableLikeMutation($tableName, $sourceTable, $this->registry, $ifNotExists);
        }

        if ($this->parser->hasCreateTableAsSelect($sql)) {
            $selectSql = $this->parser->extractCreateTableSelectSql($sql);
            $columnNames = $selectSql !== null ? (new \ZtdQuery\Platform\Postgres\Sql\Statement\SelectColumns())->extractSelectColumnNames($selectSql) : [];

            return new CreateTableAsSelectMutation(
                $tableName,
                $columnNames,
                $this->registry,
                new ColumnDeclaration(ColumnTypeFamily::TEXT, 'TEXT'),
                $ifNotExists,
            );
        }

        $definition = $this->schemaParser->parse($sql);

        return new CreateTableMutation($tableName, $definition, $this->registry, $sql, $ifNotExists);
    }

    /**
     * Resolve drop table.
     * @throws UnknownSchemaException
     * @throws UnsupportedSqlException
     */
    public function resolveDropTable(string $sql): ShadowMutation
    {
        $tableName = $this->parser->extractDropTableName($sql);
        if ($tableName === null) {
            throw new UnsupportedSqlException($sql, 'Cannot resolve table name');
        }

        $ifExists = $this->parser->hasDropTableIfExists($sql);

        if (!$ifExists && !$this->registry->has($tableName)) {
            throw new UnknownSchemaException($sql, $tableName, 'table');
        }

        return new DropTableMutation($tableName, $this->registry, $sql, $ifExists);
    }

    /**
     * Resolve alter table.
     * @throws UnknownSchemaException
     * @throws UnsupportedSqlException
     */
    public function resolveAlterTable(string $sql): ?ShadowMutation
    {
        $tableName = $this->parser->extractAlterTableName($sql);
        if ($tableName === null) {
            throw new UnsupportedSqlException($sql, 'Cannot resolve table name');
        }

        if (!$this->registry->has($tableName)) {
            throw new UnknownSchemaException($sql, $tableName, 'table');
        }

        throw new UnsupportedSqlException($sql, 'ALTER TABLE not yet supported for PostgreSQL');
    }
    /**
     * Inherits a partition parent's definition while preserving child key metadata.
     * @throws UnknownSchemaException
     * @throws UnsupportedSqlException
     */
    public function resolvePartition(string $sql, string $tableName, string $parentTable, bool $ifNotExists): ShadowMutation
    {
        $parentDefinition = $this->registry->get($parentTable);
        if ($parentDefinition === null) {
            throw new UnknownSchemaException($sql, $parentTable, 'table');
        }
        if ($parentDefinition->partitionKey === null) {
            throw new UnsupportedSqlException($sql, 'Partition parent has no partition key metadata');
        }
        $relation = $this->partitionParser->parseRelation($sql, $parentDefinition->partitionKey);
        if ($relation === null) {
            throw new UnsupportedSqlException($sql, 'Unsupported partition bound');
        }

        $definition = $parentDefinition->withPartitionRelation($relation);
        $childKey = $this->partitionParser->parseKey($sql);
        if ($childKey !== null) {
            $definition = $definition->withPartitionKey($childKey);
        }

        return new CreateTableMutation($tableName, $definition, $this->registry, $sql, $ifNotExists);
    }
}
