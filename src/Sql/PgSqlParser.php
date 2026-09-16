<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Sql;

use ZtdQuery\Platform\Postgres\Sql\Conflict\PgSqlConflictTarget;
use ZtdQuery\Platform\Postgres\Sql\Relation\PgSqlSelectRelationParser;
use ZtdQuery\Sql\SqlTokenStream;

/**
 * Focused PostgreSQL SQL parser.
 *
 * Handles the ZTD-required SQL subset: SELECT, INSERT, UPDATE, DELETE,
 * CREATE TABLE, DROP TABLE, ALTER TABLE, TRUNCATE.
 *
 * Uses regex + recursive descent hybrid approach to extract structural
 * information without a full PostgreSQL grammar parser.
 *
 * @visibility public
 * @example Identify a SELECT statement
 *     (new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser())->classifyStatement('SELECT 1') // => 'SELECT'
 */
final class PgSqlParser
{
    /**
     * Classify a SQL statement type.
     *
     * @return 'SELECT'|'INSERT'|'UPDATE'|'DELETE'|'MERGE'|'TRUNCATE'|'CREATE_TABLE'|'DROP_TABLE'|'ALTER_TABLE'|'DO'|'TCL'|null
     *
     * @visibility public
     * @example Classify the write following a common table expression
     *     (new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser())->classifyStatement('WITH ids AS (SELECT 1) DELETE FROM users WHERE id IN (SELECT * FROM ids)') // => 'DELETE'
     */
    public function classifyStatement(string $sql): ?string
    {
        return (new Statement\StatementClassifier())->classifyStatement($sql);
    }

    /**
     * Split SQL string into individual statements.
     *
     * @return list<string>
     * @visibility public
     * @example Split SQL string into individual statements
     *     (new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser())->splitStatements("SELECT ';'; SELECT 2") // => ["SELECT ';'", 'SELECT 2']
     */
    public function splitStatements(string $sql): array
    {
        return SqlTokenStream::tokenize($sql, PgSqlLexerProfile::create())->splitStatements();
    }

    /**
     * Extract table name from INSERT statement.
     * @visibility public
     * @example Extract table name from INSERT statement
     *     (new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser())->extractInsertTable('INSERT INTO public.users (id) VALUES (1)') // => 'users'
     */
    public function extractInsertTable(string $sql): ?string
    {
        return (new Statement\TargetTableParser())->extractInsertTable($sql);
    }

    /**
     * Extract column list from INSERT statement.
     *
     * @return list<string>
     * @visibility public
     * @example Extract column list from INSERT statement
     *     (new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser())->extractInsertColumns('INSERT INTO users (id, name) VALUES (1, NULL)') // => ['id', 'name']
     */
    public function extractInsertColumns(string $sql): array
    {
        return (new Dml\Insert\InsertClauseParser())->extractInsertColumns($sql);
    }

    /**
     * Extract VALUES rows from INSERT statement.
     *
     * @return list<list<string>>
     * @visibility public
     * @example Extract VALUES rows from INSERT statement
     *     (new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser())->extractInsertValues('INSERT INTO users (id) VALUES (1), (2)') // => [['1'], ['2']]
     */
    public function extractInsertValues(string $sql): array
    {
        return (new Dml\Insert\InsertClauseParser())->extractInsertValues($sql);
    }

    /**
     * Check if INSERT has ON CONFLICT clause.
     * @visibility public
     * @example Check if INSERT has ON CONFLICT clause
     *     (new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser())->hasOnConflict('INSERT INTO users VALUES (1) ON CONFLICT DO NOTHING') // => true
     */
    public function hasOnConflict(string $sql): bool
    {
        return (new Statement\ConflictClause())->hasOnConflict($sql);
    }

    /**
     * Parses ON CONFLICT columns, predicates, or a named constraint.
     * @visibility public
     * @example Parses ON CONFLICT columns, predicates, or a named constraint
     *     (new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser())->extractOnConflictTarget('INSERT INTO users VALUES (1) ON CONFLICT (id) DO NOTHING')?->columns // => ['id']
     */
    public function extractOnConflictTarget(string $sql): ?PgSqlConflictTarget
    {
        return (new Statement\ConflictClause())->extractOnConflictTarget($sql);
    }

    /**
     * Extract ON CONFLICT ... DO UPDATE SET columns and values.
     *
     * @return array{columns: list<string>, values: array<string, string>}
     * @visibility public
     * @example Extract ON CONFLICT ... DO UPDATE SET columns and values
     *     (new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser())->extractOnConflictUpdateColumns('INSERT INTO users VALUES (1) ON CONFLICT (id) DO UPDATE SET id = 2') // => ['columns' => ['id'], 'values' => ['id' => '2']]
     */
    public function extractOnConflictUpdateColumns(string $sql): array
    {
        return (new Statement\ConflictClause())->extractOnConflictUpdateColumns($sql);
    }

    /**
     * Returns the predicate limiting an ON CONFLICT update, when present.
     * @visibility public
     * @example Returns the predicate limiting an ON CONFLICT update, when present
     *     (new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser())->extractOnConflictUpdateWhere('INSERT INTO users VALUES (1) ON CONFLICT (id) DO UPDATE SET id = 2 WHERE users.id = 1') // => 'users.id = 1'
     */
    public function extractOnConflictUpdateWhere(string $sql): ?string
    {
        return (new Statement\ConflictClause())->extractOnConflictUpdateWhere($sql);
    }

    /**
     * Check if INSERT has a SELECT subquery (INSERT ... SELECT).
     * @visibility public
     * @example Check if INSERT has a SELECT subquery (INSERT ... SELECT)
     *     (new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser())->hasInsertSelect('INSERT INTO users (id) SELECT id FROM archive') // => true
     */
    public function hasInsertSelect(string $sql): bool
    {
        return (new Dml\Insert\InsertClauseParser())->hasInsertSelect($sql);
    }

    /**
     * Extract the SELECT part from INSERT ... SELECT.
     * @visibility public
     * @example Extract the SELECT part from INSERT ... SELECT
     *     (new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser())->extractInsertSelectSql('INSERT INTO users (id) SELECT id FROM archive') // => 'SELECT id FROM archive'
     */
    public function extractInsertSelectSql(string $sql): ?string
    {
        return (new Dml\Insert\InsertClauseParser())->extractInsertSelectSql($sql);
    }

    /**
     * Extract table name from UPDATE statement.
     * @visibility public
     * @example Extract table name from UPDATE statement
     *     (new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser())->extractUpdateTable('UPDATE public.users SET id = 2') // => 'users'
     */
    public function extractUpdateTable(string $sql): ?string
    {
        return (new Statement\TargetTableParser())->extractUpdateTable($sql);
    }

    /**
     * Extract table alias from UPDATE statement.
     * @visibility public
     * @example Extract table alias from UPDATE statement
     *     (new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser())->extractUpdateAlias('UPDATE users AS u SET id = 2') // => 'u'
     */
    public function extractUpdateAlias(string $sql): ?string
    {
        return (new Dml\Update\UpdateClauseParser())->extractUpdateAlias($sql);
    }

    /**
     * Extract SET assignments from UPDATE statement.
     *
     * @return array<string, string> column => value expression
     * @visibility public
     * @example Extract SET assignments from UPDATE statement
     *     (new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser())->extractUpdateSets('UPDATE users SET id = 2, name = NULL WHERE id = 1') // => ['id' => '2', 'name' => 'NULL']
     */
    public function extractUpdateSets(string $sql): array
    {
        return (new Dml\Update\UpdateClauseParser())->extractUpdateSets($sql);
    }

    /**
     * Extract WHERE clause from UPDATE or DELETE statement.
     * @visibility public
     * @example Extract WHERE clause from UPDATE or DELETE statement
     *     (new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser())->extractWhereClause('DELETE FROM users WHERE id = 1 RETURNING id') // => 'id = 1'
     */
    public function extractWhereClause(string $sql): ?string
    {
        return SqlTokenStream::tokenize($sql, PgSqlLexerProfile::create())->topLevelClause(
            ['WHERE'],
            [['RETURNING'], ['ORDER', 'BY'], ['LIMIT']],
        );
    }

    /**
     * Extract FROM clause from UPDATE statement (PostgreSQL extension).
     * @visibility public
     * @example Extract FROM clause from UPDATE statement (PostgreSQL extension)
     *     (new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser())->extractUpdateFromClause('UPDATE users SET id = a.id FROM archive AS a WHERE users.id = a.id') // => 'archive AS a'
     */
    public function extractUpdateFromClause(string $sql): ?string
    {
        return SqlTokenStream::tokenize($sql, PgSqlLexerProfile::create())->topLevelClause(
            ['FROM'],
            [['WHERE'], ['RETURNING']],
        );
    }

    /**
     * Extract table name from DELETE statement.
     * @visibility public
     * @example Extract table name from DELETE statement
     *     (new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser())->extractDeleteTable('DELETE FROM public.users WHERE id = 1') // => 'users'
     */
    public function extractDeleteTable(string $sql): ?string
    {
        return (new Statement\TargetTableParser())->extractDeleteTable($sql);
    }

    /**
     * Extract table alias from DELETE statement.
     * @visibility public
     * @example Extract table alias from DELETE statement
     *     (new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser())->extractDeleteAlias('DELETE FROM users AS u WHERE u.id = 1') // => 'u'
     */
    public function extractDeleteAlias(string $sql): ?string
    {
        return (new Dml\Delete\DeleteClauseParser())->extractDeleteAlias($sql);
    }

    /**
     * Extract USING clause from DELETE statement.
     * @visibility public
     * @example Extract USING clause from DELETE statement
     *     (new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser())->extractDeleteUsingClause('DELETE FROM users USING archive WHERE users.id = archive.id') // => 'archive'
     */
    public function extractDeleteUsingClause(string $sql): ?string
    {
        return SqlTokenStream::tokenize($sql, PgSqlLexerProfile::create())->topLevelClause(
            ['USING'],
            [['WHERE'], ['RETURNING']],
        );
    }

    /**
     * Extract table name from TRUNCATE statement.
     * @visibility public
     * @example Extract table name from TRUNCATE statement
     *     (new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser())->extractTruncateTable('TRUNCATE TABLE users, archive') // => 'users'
     */
    public function extractTruncateTable(string $sql): ?string
    {
        return (new Statement\TableDefinitionClauses())->extractTruncateTables($sql)[0] ?? null;
    }

    /**
     * @return list<string>
     * @visibility public
     * @example Extract every table in a TRUNCATE statement
     *     (new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser())->extractTruncateTables('TRUNCATE TABLE users, archive') // => ['users', 'archive']
     */
    public function extractTruncateTables(string $sql): array
    {
        return (new Statement\TableDefinitionClauses())->extractTruncateTables($sql);
    }

    /**
     * Extract table name from CREATE TABLE statement.
     * @visibility public
     * @example Extract table name from CREATE TABLE statement
     *     (new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser())->extractCreateTableName('CREATE TABLE IF NOT EXISTS users (id INTEGER)') // => 'users'
     */
    public function extractCreateTableName(string $sql): ?string
    {
        return (new Statement\TableDefinitionClauses())->extractCreateTableName($sql);
    }

    /**
     * Check if CREATE TABLE has IF NOT EXISTS.
     * @visibility public
     * @example Check if CREATE TABLE has IF NOT EXISTS
     *     (new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser())->hasIfNotExists('CREATE TABLE IF NOT EXISTS users (id INTEGER)') // => true
     */
    public function hasIfNotExists(string $sql): bool
    {
        return (new Statement\TableDefinitionClauses())->hasIfNotExists($sql);
    }

    /**
     * Check if CREATE TABLE has AS SELECT.
     * @visibility public
     * @example Check if CREATE TABLE has AS SELECT
     *     (new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser())->hasCreateTableAsSelect('CREATE TABLE archive AS SELECT id FROM users') // => true
     */
    public function hasCreateTableAsSelect(string $sql): bool
    {
        return (new Statement\TableDefinitionClauses())->hasCreateTableAsSelect($sql);
    }

    /**
     * Extract the SELECT SQL from CREATE TABLE ... AS SELECT.
     * @visibility public
     * @example Extract the SELECT SQL from CREATE TABLE ... AS SELECT
     *     (new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser())->extractCreateTableSelectSql('CREATE TABLE archive AS SELECT id FROM users') // => 'SELECT id FROM users'
     */
    public function extractCreateTableSelectSql(string $sql): ?string
    {
        return (new Statement\TableDefinitionClauses())->extractCreateTableSelectSql($sql);
    }

    /**
     * Check if CREATE TABLE has LIKE clause.
     * @visibility public
     * @example Check if CREATE TABLE has LIKE clause
     *     (new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser())->hasCreateTableLike('CREATE TABLE archive (LIKE users INCLUDING ALL)') // => true
     */
    public function hasCreateTableLike(string $sql): bool
    {
        return (new Statement\TableDefinitionClauses())->hasCreateTableLike($sql);
    }

    /**
     * Extract the LIKE source table name.
     * @visibility public
     * @example Extract the LIKE source table name
     *     (new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser())->extractCreateTableLikeSource('CREATE TABLE archive (LIKE users INCLUDING ALL)') // => 'users'
     */
    public function extractCreateTableLikeSource(string $sql): ?string
    {
        return (new Statement\TableDefinitionClauses())->extractCreateTableLikeSource($sql);
    }

    /**
     * Extract table name from DROP TABLE statement.
     * @visibility public
     * @example Extract table name from DROP TABLE statement
     *     (new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser())->extractDropTableName('DROP TABLE IF EXISTS users') // => 'users'
     */
    public function extractDropTableName(string $sql): ?string
    {
        return (new Statement\TableDefinitionClauses())->extractDropTableName($sql);
    }

    /**
     * Check if DROP TABLE has IF EXISTS.
     * @visibility public
     * @example Check if DROP TABLE has IF EXISTS
     *     (new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser())->hasDropTableIfExists('DROP TABLE IF EXISTS users') // => true
     */
    public function hasDropTableIfExists(string $sql): bool
    {
        return (new Statement\TableDefinitionClauses())->hasDropTableIfExists($sql);
    }

    /**
     * Extract table name from ALTER TABLE statement.
     * @visibility public
     * @example Extract table name from ALTER TABLE statement
     *     (new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser())->extractAlterTableName('ALTER TABLE users ADD COLUMN name TEXT') // => 'users'
     */
    public function extractAlterTableName(string $sql): ?string
    {
        return (new Statement\TableDefinitionClauses())->extractAlterTableName($sql);
    }

    /**
     * Unquote a PostgreSQL identifier (remove double quotes).
     * @visibility public
     * @example Unquote a PostgreSQL identifier (remove double quotes)
     *     (new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser())->unquoteIdentifier('"order""items"') // => 'order"items'
     */
    public function unquoteIdentifier(string $identifier): string
    {
        return (new Lexing\IdentifierDecoder())->unquoteIdentifier($identifier);
    }

    /**
     * Strip schema prefix from a potentially schema-qualified name.
     * "public"."users" -> "users", public.users -> users
     * @visibility public
     * @example Strip schema prefix from a potentially schema-qualified name
     *     (new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser())->stripSchemaPrefix('public.users') // => 'users'
     */
    public function stripSchemaPrefix(string $name): string
    {
        return (new Lexing\IdentifierDecoder())->stripSchemaPrefix($name);
    }

    /**
     * Extract table names referenced in a SELECT statement.
     *
     * @return list<string>
     * @visibility public
     * @example Extract table names referenced in a SELECT statement
     *     (new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser())->extractSelectTableNames('SELECT u.id FROM users u JOIN orders o ON u.id = o.user_id') // => ['users', 'orders']
     */
    public function extractSelectTableNames(string $sql): array
    {
        return (new PgSqlSelectRelationParser())->tableNames($sql);
    }
}
