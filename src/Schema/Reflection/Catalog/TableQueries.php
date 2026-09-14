<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Schema\Reflection\Catalog;

use ZtdQuery\Connection\ConnectionInterface;
use ZtdQuery\Connection\StatementInterface;

/**
 * Runs the ordered catalog queries needed to reconstruct a table declaration.
 *
 * @visibility root
 */
final class TableQueries
{
    /**
     * Supplies the connection used to inspect the current PostgreSQL schema.
     */
    public function __construct(private readonly ConnectionInterface $connection)
    {
    }

    /**
     * Fetches columns metadata in its catalog order.
     */
    public function columns(string $tableName): StatementInterface|false
    {
        $escapedTableName = str_replace("'", "''", $tableName);
        return $this->connection->query(
            'SELECT column_name, data_type, character_maximum_length, '
            . 'numeric_precision, numeric_scale, is_nullable, column_default, '
            . 'udt_name, domain_schema, domain_name, is_identity, identity_generation, '
            . 'is_generated, generation_expression '
            . 'FROM information_schema.columns '
            . "WHERE table_schema = current_schema() AND table_name = '" . $escapedTableName . "' "
            . 'ORDER BY ordinal_position'
        );
    }

    /**
     * Fetches primary key metadata in its catalog order.
     */
    public function primaryKey(string $tableName): StatementInterface|false
    {
        $escapedTableName = str_replace("'", "''", $tableName);
        return $this->connection->query(
            'SELECT kcu.column_name '
            . 'FROM information_schema.table_constraints tc '
            . 'JOIN information_schema.key_column_usage kcu '
            . '  ON tc.constraint_name = kcu.constraint_name '
            . '  AND tc.table_schema = kcu.table_schema '
            . 'WHERE tc.table_schema = current_schema() '
            . "  AND tc.table_name = '" . $escapedTableName . "' "
            . "  AND tc.constraint_type = 'PRIMARY KEY' "
            . 'ORDER BY kcu.ordinal_position'
        );
    }

    /**
     * Fetches unique indexes metadata in its catalog order.
     */
    public function uniqueIndexes(string $tableName): StatementInterface|false
    {
        $escapedTableName = str_replace("'", "''", $tableName);
        return $this->connection->query(
            'SELECT index_relation.relname AS constraint_name, attribute.attname AS column_name, '
            . 'pg_get_expr(index_metadata.indpred, index_metadata.indrelid) AS predicate '
            . 'FROM pg_catalog.pg_class table_relation '
            . 'JOIN pg_catalog.pg_namespace namespace ON namespace.oid = table_relation.relnamespace '
            . 'JOIN pg_catalog.pg_index index_metadata ON index_metadata.indrelid = table_relation.oid '
            . 'JOIN pg_catalog.pg_class index_relation ON index_relation.oid = index_metadata.indexrelid '
            . 'JOIN LATERAL unnest(index_metadata.indkey) WITH ORDINALITY key_column(attnum, ordinality) '
            . '  ON key_column.ordinality <= index_metadata.indnkeyatts '
            . 'LEFT JOIN pg_catalog.pg_attribute attribute '
            . '  ON attribute.attrelid = table_relation.oid AND attribute.attnum = key_column.attnum '
            . 'WHERE namespace.nspname = current_schema() '
            . "  AND table_relation.relname = '" . $escapedTableName . "' "
            . '  AND index_metadata.indisunique '
            . '  AND index_metadata.indisvalid '
            . '  AND NOT index_metadata.indisprimary '
            . 'ORDER BY index_relation.relname, key_column.ordinality'
        );
    }

    /**
     * Fetches foreign keys metadata in its catalog order.
     */
    public function foreignKeys(string $tableName): StatementInterface|false
    {
        $escapedTableName = str_replace("'", "''", $tableName);
        return $this->connection->query(
            'SELECT fk.constraint_name, fk.column_name, '
            . 'pk.table_name AS foreign_table_name, pk.column_name AS foreign_column_name, '
            . 'rc.update_rule, rc.delete_rule '
            . 'FROM information_schema.referential_constraints rc '
            . 'JOIN information_schema.key_column_usage fk '
            . '  ON fk.constraint_catalog = rc.constraint_catalog '
            . '  AND fk.constraint_schema = rc.constraint_schema '
            . '  AND fk.constraint_name = rc.constraint_name '
            . 'JOIN information_schema.key_column_usage pk '
            . '  ON pk.constraint_catalog = rc.unique_constraint_catalog '
            . '  AND pk.constraint_schema = rc.unique_constraint_schema '
            . '  AND pk.constraint_name = rc.unique_constraint_name '
            . '  AND pk.ordinal_position = fk.position_in_unique_constraint '
            . 'WHERE fk.table_schema = current_schema() '
            . "  AND fk.table_name = '" . $escapedTableName . "' "
            . 'ORDER BY fk.constraint_name, fk.ordinal_position'
        );
    }
}
