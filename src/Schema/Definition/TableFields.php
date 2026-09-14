<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Schema\Definition;

use ZtdQuery\Platform\Postgres\Sql\Partition\PgSqlPartitionParser;
use ZtdQuery\Schema\ColumnDeclaration;
use ZtdQuery\Schema\Key\IdentityGenerationStrategy;
use ZtdQuery\Schema\TableDefinition;

/**
 * Accumulates a CREATE TABLE declaration before validating its complete column set.
 *
 * @visibility root
 */
final class TableFields
{
    /**
     * @var list<string>
     */
    private array $columns = [];
    /**
     * @var array<string, string>
     */
    private array $columnTypes = [];
    /**
     * @var array<string, ColumnDeclaration>
     */
    private array $typedColumns = [];
    /**
     * @var array<string, string>
     */
    private array $columnDefaults = [];
    /**
     * @var array<string, IdentityGenerationStrategy>
     */
    private array $identityStrategies = [];
    /**
     * @var array<string, string>
     */
    private array $generatedExpressions = [];
    /**
     * @var list<string>
     */
    private array $primaryKeys = [];
    /**
     * @var list<string>
     */
    private array $notNullColumns = [];
    /**
     * @var array<string, list<string>>
     */
    private array $uniqueConstraints = [];
    private int $uniqueIndex = 0;

    /**
     * Adds a column or table constraint in declaration order.
     */
    public function appendEntry(string $entry): void
    {
        $entry = trim($entry);
        if ($entry === '') {
            return;
        }

        if ((new TableConstraint())->isConstraintEntry($entry)) {
            (new TableConstraint())->parseConstraint($entry, $this->primaryKeys, $this->uniqueConstraints, $this->uniqueIndex);
            return;
        }

        $columnDef = (new ColumnDefinition())->parseColumnDefinition($entry);
        if ($columnDef === null) {
            return;
        }

        $this->appendColumn($columnDef);
    }

    /**
     * Records typed column metadata and its inline constraints.
     * @param array{name: string, type: string, columnType: ColumnDeclaration, notNull: bool, primaryKey: bool, unique: bool, default: string|null, identity: bool, generatedExpression: string|null} $columnDef
     */
    public function appendColumn(array $columnDef): void
    {
        $this->columns[] = $columnDef['name'];
        $this->columnTypes[$columnDef['name']] = $columnDef['type'];
        $this->typedColumns[$columnDef['name']] = $columnDef['columnType'];

        if ($columnDef['notNull']) {
            $this->notNullColumns[] = $columnDef['name'];
        }

        if ($columnDef['primaryKey']) {
            $this->primaryKeys[] = $columnDef['name'];
            if (!in_array($columnDef['name'], $this->notNullColumns, true)) {
                $this->notNullColumns[] = $columnDef['name'];
            }
        }

        if ($columnDef['unique']) {
            $keyName = $columnDef['name'] . '_UNIQUE';
            $this->uniqueConstraints[$keyName] = [$columnDef['name']];
        }

        if ($columnDef['default'] !== null && !ColumnDefinition::isSequenceDefault($columnDef['default'])) {
            $this->columnDefaults[$columnDef['name']] = $columnDef['default'];
        }
        if ($columnDef['identity']) {
            $this->identityStrategies[$columnDef['name']] = IdentityGenerationStrategy::Sequence;
        }
        if ($columnDef['generatedExpression'] !== null) {
            $this->generatedExpressions[$columnDef['name']] = $columnDef['generatedExpression'];
        }
    }

    /**
     * Validates referenced primary and unique columns and produces the complete table definition.
     * @param array<string, \ZtdQuery\Schema\Key\ForeignKeyDefinition> $foreignKeys
     */
    public function definition(string $createTableSql, array $foreignKeys): ?TableDefinition
    {
        if ($this->columns === []) {
            return null;
        }

        foreach ([$this->primaryKeys, ...array_values($this->uniqueConstraints)] as $constraintColumns) {
            foreach ($constraintColumns as $col) {
                if (!in_array($col, $this->columns, true)) {
                    return null;
                }
            }
        }

        return new TableDefinition(
            $this->columns,
            $this->columnTypes,
            $this->primaryKeys,
            $this->notNullColumns,
            $this->uniqueConstraints,
            $this->typedColumns,
            $this->columnDefaults,
            $this->identityStrategies,
            $this->generatedExpressions,
            $foreignKeys,
            partitionKey: (new PgSqlPartitionParser())->parseKey($createTableSql),
        );
    }
}
