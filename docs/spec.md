# PostgreSQL SQL Specification for ZTD

This document defines how ZTD (Zero Table Dependency) handles PostgreSQL SQL statements. ZTD simulates query results without modifying the physical database by using CTEs to shadow tables and virtualize writes.

**Grammar Reference:** PostgreSQL 17.2 official Bison grammar (`gram.y`). The `stmt` rule defines 125 statement alternatives (124 named + 1 empty production). This spec covers all 124 named alternatives.

## Overview

ZTD categorizes SQL statements into three handling modes:

| Mode | Description |
|------|-------------|
| **Rewrite** | Transform the query using CTE shadowing to simulate the operation |
| **Ignored** | Statement has no effect (by design, e.g., transaction control) |
| **Unsupported** | Follows Unsupported SQL behavior (ignore/notice/exception/passthrough) |

---

## DML (Data Manipulation Language)

Grammar rules: `SelectStmt`, `InsertStmt`, `UpdateStmt`, `DeleteStmt`, `MergeStmt`, `TruncateStmt`

### SELECT (SelectStmt)

| Mode | Behavior |
|------|----------|
| Rewrite | Apply CTE shadowing to inject virtual mutations from ShadowStore |

**Grammar:** `SelectStmt` -> `select_no_parens` | `select_with_parens`. `simple_select` has 7 alternatives including basic SELECT, VALUES, TABLE, set operations (UNION/INTERSECT/EXCEPT), and parenthesized forms.

**Supported Syntax:**
- `SELECT ... FROM ...` - Basic select
- `SELECT ... WHERE ...` - Conditional select
- `SELECT ... JOIN ...` - Join operations (INNER, LEFT, RIGHT, FULL, CROSS, NATURAL)
- `SELECT ... GROUP BY ...` - Aggregation
- `SELECT ... HAVING ...` - Aggregate filtering
- `SELECT ... ORDER BY ...` - Sorting
- `SELECT ... LIMIT ... OFFSET ...` / `SELECT ... FETCH FIRST ...` - Pagination
- `SELECT ... UNION [ALL] ...` - Set union
- `SELECT ... EXCEPT [ALL] ...` - Set difference
- `SELECT ... INTERSECT [ALL] ...` - Set intersection
- `SELECT ... (subquery)` - Subqueries
- `SELECT DISTINCT ...` / `SELECT DISTINCT ON (...) ...` - Distinct filtering
- `WITH ... SELECT ...` - Common Table Expressions
- `WITH RECURSIVE ... SELECT ...` - Recursive CTEs
- `SELECT ... WINDOW ...` - Window function definitions
- `SELECT ... FOR UPDATE` - Row locking (lock ignored, CTE applied)
- `SELECT ... FOR SHARE` - Shared locking (lock ignored, CTE applied)
- `SELECT ... FOR NO KEY UPDATE` - Row locking (lock ignored, CTE applied)
- `SELECT ... FOR KEY SHARE` - Shared locking (lock ignored, CTE applied)
- `VALUES (...)` - Values expression (standalone)
- `TABLE ...` - Table expression (equivalent to SELECT * FROM ...)

**Unsupported Syntax:**
- `SELECT ... INTO ...` - INTO clause for creating tables (use CREATE TABLE AS)

### INSERT (InsertStmt)

| Mode | Behavior |
|------|----------|
| Rewrite | Convert to result-select query, store mutation in ShadowStore |

**Grammar:** `InsertStmt` -> `opt_with_clause INSERT INTO insert_target insert_rest opt_on_conflict returning_clause`. `insert_rest` has 5 alternatives: SELECT, VALUES, SET DEFAULT, and parenthesized SELECT.

**Transformation:**
```sql
-- Original
INSERT INTO users (id, name) VALUES (1, 'Alice')

-- Transformed (Result Select)
SELECT 1 AS "id", 'Alice' AS "name"
```

**Supported Syntax:**
- `INSERT INTO ... VALUES (...)` - Single row insert
- `INSERT INTO ... VALUES (...), (...)` - Multi-row insert
- `INSERT ... SELECT ...` - Insert from subquery
- `INSERT ... ON CONFLICT ... DO UPDATE SET ...` - Upsert (PostgreSQL-native)
- `INSERT ... ON CONFLICT ... DO NOTHING` - Skip conflicts
- `INSERT INTO ONLY ...` - Inheritance-aware insert (ONLY keyword accepted)
- `INSERT INTO ... OVERRIDING SYSTEM VALUE ...` - Identity column override
- `INSERT INTO ... OVERRIDING USER VALUE ...` - Identity column override
- `WITH ... INSERT ...` - CTE with INSERT
- `INSERT INTO ... DEFAULT VALUES` - Insert defaults only

**PostgreSQL-Specific Upsert:**
```sql
-- Original
INSERT INTO users (id, name) VALUES (1, 'Alice')
ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name

-- Resolved as UpsertMutation with conflict columns and EXCLUDED references
```

**Unsupported Syntax:**
- `INSERT ... RETURNING ...` - RETURNING clause is stripped; Result Select approach is used instead

### UPDATE (UpdateStmt)

| Mode | Behavior |
|------|----------|
| Rewrite | Convert to result-select query, store mutation in ShadowStore |

**Grammar:** `UpdateStmt` -> `opt_with_clause UPDATE relation_expr_opt_alias SET set_clause_list from_clause where_or_current_clause returning_clause`

**Transformation:**
```sql
-- Original
UPDATE users SET name = 'Bob' WHERE id = 1

-- Transformed (Result Select with CTE shadowing)
WITH "users" AS MATERIALIZED (
  SELECT * FROM (VALUES
    (CAST(1 AS INTEGER), CAST('Alice' AS TEXT))
  ) AS t("id", "name")
)
SELECT 'Bob' AS "name", "users"."id" FROM "users" WHERE id = 1
```

**Supported Syntax:**
- `UPDATE ... SET ... WHERE ...` - Single table update
- `UPDATE ONLY ... SET ...` - Inheritance-aware update (ONLY keyword accepted)
- `UPDATE ... SET ... FROM ...` - Multi-table update (PostgreSQL extension)
- `UPDATE ... AS alias SET ...` - Table alias
- `WITH ... UPDATE ...` - CTE with UPDATE
- `UPDATE ... SET (col1, col2) = (val1, val2)` - Multi-column assignment
- `UPDATE ... SET (col1, col2) = (SELECT ...)` - Subquery assignment

**Unsupported Syntax:**
- `UPDATE ... RETURNING ...` - RETURNING clause is stripped; Result Select approach is used instead
- `UPDATE ... WHERE CURRENT OF cursor` - Cursor-based update

### DELETE (DeleteStmt)

| Mode | Behavior |
|------|----------|
| Rewrite | Convert to result-select query, store mutation in ShadowStore |

**Grammar:** `DeleteStmt` -> `opt_with_clause DELETE FROM relation_expr_opt_alias using_clause where_or_current_clause returning_clause`

**Transformation:**
```sql
-- Original
DELETE FROM users WHERE id = 1

-- Transformed (Result Select with CTE shadowing)
WITH "users" AS MATERIALIZED (
  SELECT * FROM (VALUES
    (CAST(1 AS INTEGER), CAST('Alice' AS TEXT))
  ) AS t("id", "name")
)
SELECT "users"."id" AS "id", "users"."name" AS "name" FROM "users" WHERE id = 1
```

**Supported Syntax:**
- `DELETE FROM ... WHERE ...` - Single table delete
- `DELETE FROM ONLY ...` - Inheritance-aware delete (ONLY keyword accepted)
- `DELETE FROM ... AS alias ...` - Table alias
- `DELETE FROM ... USING ...` - Join delete (PostgreSQL extension)
- `WITH ... DELETE ...` - CTE with DELETE

**Unsupported Syntax:**
- `DELETE ... RETURNING ...` - RETURNING clause is stripped; Result Select approach is used instead
- `DELETE ... WHERE CURRENT OF cursor` - Cursor-based delete

### MERGE (MergeStmt)

| Mode | Behavior |
|------|----------|
| Unsupported | Follows Unsupported SQL behavior (ignore/notice/exception) |

**Grammar:** `MergeStmt` -> `opt_with_clause MERGE INTO ...`

**Reason:** MERGE combines INSERT, UPDATE, and DELETE semantics in a single statement. Simulating all WHEN MATCHED / WHEN NOT MATCHED branches requires complex conditional logic beyond the current mutation resolver.

### TRUNCATE (TruncateStmt)

| Mode | Behavior |
|------|----------|
| Rewrite | Clear all virtual data for the table in ShadowStore |

**Grammar:** `TruncateStmt` -> `TRUNCATE opt_table relation_expr_list opt_restart_seqs opt_drop_behavior`

**Supported Syntax:**
- `TRUNCATE TABLE ...`
- `TRUNCATE ...` (TABLE keyword optional in PostgreSQL)
- `TRUNCATE TABLE ONLY ...`
- `TRUNCATE ... CASCADE` / `TRUNCATE ... RESTRICT`
- `TRUNCATE ... RESTART IDENTITY` / `TRUNCATE ... CONTINUE IDENTITY`

### Unsupported DML

| Grammar Rule | SQL Statement | Mode | Reason |
|-------------|---------------|------|--------|
| `CopyStmt` | `COPY ... FROM/TO ...` | Unsupported | Bulk copy to/from files; requires file system access |
| `DoStmt` | `DO $$ ... $$` | Unsupported | Anonymous PL/pgSQL block; opaque execution |
| `CallStmt` | `CALL procedure(...)` | Unsupported | Stored procedure invocation; opaque execution |

---

## DDL (Data Definition Language)

ZTD manages DDL virtually within SchemaRegistry. Physical database schema is never modified.

Grammar rules: PostgreSQL has 66 DDL-related `stmt` alternatives covering tables, indexes, views, functions, types, schemas, sequences, triggers, rules, and more. Only table-related DDL is rewritten; all other DDL is unsupported.

### CREATE TABLE (CreateStmt, CreateAsStmt)

| Mode | Behavior |
|------|----------|
| Rewrite | Register virtual table in SchemaRegistry |

**Grammar:** `CreateStmt` (6 alternatives) handles `CREATE TABLE` with column definitions. `CreateAsStmt` (2 alternatives) handles `CREATE TABLE ... AS SELECT`.

**Supported Syntax:**
- `CREATE TABLE ...` - Create virtual table
- `CREATE TABLE IF NOT EXISTS ...` - Conditional create
- `CREATE TABLE ... (LIKE ...)` - Copy structure from existing table
- `CREATE TABLE ... AS SELECT ...` - Create from query result
- `CREATE TEMPORARY TABLE ...` - Temporary table (treated as normal)
- `CREATE TEMP TABLE ...` - Short form of TEMPORARY
- `CREATE UNLOGGED TABLE ...` - Unlogged table (treated as normal)

### ALTER TABLE (AlterTableStmt)

| Mode | Behavior |
|------|----------|
| Unsupported | ALTER TABLE is not yet supported for PostgreSQL |

**Grammar:** `AlterTableStmt` has 21 alternatives covering tables, materialized views, indexes, sequences, and foreign tables. `alter_table_cmd` has 61 sub-alternatives for individual operations.

**Design Decision:** The regex-based parser classifies ALTER TABLE but the mutation resolver does not yet implement ALTER TABLE operations. ALTER TABLE statements follow Unsupported SQL behavior.

### DROP TABLE (DropStmt)

| Mode | Behavior |
|------|----------|
| Rewrite | Remove virtual table from SchemaRegistry |

**Grammar:** `DropStmt` has 12 alternatives covering multiple object types. Only `DROP TABLE` is rewritten.

**Supported Syntax:**
- `DROP TABLE ...`
- `DROP TABLE IF EXISTS ...`
- `DROP TABLE ... CASCADE` / `DROP TABLE ... RESTRICT`

### Other DDL (Unsupported)

All non-table DDL follows Unsupported SQL behavior. The complete list from the grammar:

| Grammar Rule | SQL Statement | Category |
|-------------|---------------|----------|
| **Index** | | |
| `IndexStmt` | `CREATE [UNIQUE] INDEX ...` | Index |
| `ReindexStmt` | `REINDEX ...` | Index |
| **View** | | |
| `ViewStmt` | `CREATE [OR REPLACE] VIEW ...` | View |
| `CreateMatViewStmt` | `CREATE MATERIALIZED VIEW ...` | Materialized View |
| `RefreshMatViewStmt` | `REFRESH MATERIALIZED VIEW ...` | Materialized View |
| **Schema / Database** | | |
| `CreateSchemaStmt` | `CREATE SCHEMA ...` | Schema |
| `CreatedbStmt` | `CREATE DATABASE ...` | Database |
| `AlterDatabaseStmt` | `ALTER DATABASE ...` | Database |
| `AlterDatabaseSetStmt` | `ALTER DATABASE ... SET ...` | Database |
| `DropdbStmt` | `DROP DATABASE ...` | Database |
| **Sequence** | | |
| `CreateSeqStmt` | `CREATE SEQUENCE ...` | Sequence |
| `AlterSeqStmt` | `ALTER SEQUENCE ...` | Sequence |
| **Function / Procedure** | | |
| `CreateFunctionStmt` | `CREATE [OR REPLACE] FUNCTION/PROCEDURE ...` | Function |
| `AlterFunctionStmt` | `ALTER FUNCTION/PROCEDURE ...` | Function |
| `RemoveFuncStmt` | `DROP FUNCTION/PROCEDURE ...` | Function |
| `RemoveAggrStmt` | `DROP AGGREGATE ...` | Function |
| `RemoveOperStmt` | `DROP OPERATOR ...` | Operator |
| **Type / Domain / Enum** | | |
| `DefineStmt` | `CREATE TYPE/OPERATOR/AGGREGATE/COLLATION ...` | Type/Operator |
| `CreateDomainStmt` | `CREATE DOMAIN ...` | Domain |
| `AlterDomainStmt` | `ALTER DOMAIN ...` | Domain |
| `AlterEnumStmt` | `ALTER TYPE ... ADD VALUE ...` | Enum |
| `AlterTypeStmt` | `ALTER TYPE ... ALTER ATTRIBUTE ...` | Type |
| `AlterCompositeTypeStmt` | `ALTER TYPE ... (composite)` | Type |
| `AlterCollationStmt` | `ALTER COLLATION ...` | Collation |
| **Trigger / Rule / Event Trigger** | | |
| `CreateTrigStmt` | `CREATE TRIGGER ...` | Trigger |
| `RuleStmt` | `CREATE RULE ...` | Rule |
| `CreateEventTrigStmt` | `CREATE EVENT TRIGGER ...` | Event Trigger |
| `AlterEventTrigStmt` | `ALTER EVENT TRIGGER ...` | Event Trigger |
| **Extension** | | |
| `CreateExtensionStmt` | `CREATE EXTENSION ...` | Extension |
| `AlterExtensionStmt` | `ALTER EXTENSION ...` | Extension |
| `AlterExtensionContentsStmt` | `ALTER EXTENSION ... ADD/DROP ...` | Extension |
| **Foreign Data** | | |
| `CreateFdwStmt` | `CREATE FOREIGN DATA WRAPPER ...` | FDW |
| `AlterFdwStmt` | `ALTER FOREIGN DATA WRAPPER ...` | FDW |
| `CreateForeignServerStmt` | `CREATE SERVER ...` | Foreign Server |
| `AlterForeignServerStmt` | `ALTER SERVER ...` | Foreign Server |
| `CreateForeignTableStmt` | `CREATE FOREIGN TABLE ...` | Foreign Table |
| `CreateUserMappingStmt` | `CREATE USER MAPPING ...` | User Mapping |
| `AlterUserMappingStmt` | `ALTER USER MAPPING ...` | User Mapping |
| `DropUserMappingStmt` | `DROP USER MAPPING ...` | User Mapping |
| `ImportForeignSchemaStmt` | `IMPORT FOREIGN SCHEMA ...` | Foreign Schema |
| **Tablespace** | | |
| `CreateTableSpaceStmt` | `CREATE TABLESPACE ...` | Tablespace |
| `AlterTblSpcStmt` | `ALTER TABLESPACE ...` | Tablespace |
| `DropTableSpaceStmt` | `DROP TABLESPACE ...` | Tablespace |
| **Policy / Statistics** | | |
| `CreatePolicyStmt` | `CREATE POLICY ...` | Policy |
| `AlterPolicyStmt` | `ALTER POLICY ...` | Policy |
| `CreateStatsStmt` | `CREATE STATISTICS ...` | Statistics |
| `AlterStatsStmt` | `ALTER STATISTICS ...` | Statistics |
| **Publication / Subscription** | | |
| `CreatePublicationStmt` | `CREATE PUBLICATION ...` | Publication |
| `AlterPublicationStmt` | `ALTER PUBLICATION ...` | Publication |
| `CreateSubscriptionStmt` | `CREATE SUBSCRIPTION ...` | Subscription |
| `AlterSubscriptionStmt` | `ALTER SUBSCRIPTION ...` | Subscription |
| `DropSubscriptionStmt` | `DROP SUBSCRIPTION ...` | Subscription |
| **Transform / Language / Conversion** | | |
| `CreateTransformStmt` | `CREATE TRANSFORM ...` | Transform |
| `DropTransformStmt` | `DROP TRANSFORM ...` | Transform |
| `CreatePLangStmt` | `CREATE [TRUSTED] LANGUAGE ...` | Language |
| `CreateConversionStmt` | `CREATE CONVERSION ...` | Conversion |
| `CreateAmStmt` | `CREATE ACCESS METHOD ...` | Access Method |
| `CreateCastStmt` | `CREATE CAST ...` | Cast |
| `DropCastStmt` | `DROP CAST ...` | Cast |
| **Operator Class/Family** | | |
| `CreateOpClassStmt` | `CREATE OPERATOR CLASS ...` | Op Class |
| `CreateOpFamilyStmt` | `CREATE OPERATOR FAMILY ...` | Op Family |
| `AlterOpFamilyStmt` | `ALTER OPERATOR FAMILY ...` | Op Family |
| `DropOpClassStmt` | `DROP OPERATOR CLASS ...` | Op Class |
| `DropOpFamilyStmt` | `DROP OPERATOR FAMILY ...` | Op Family |
| `AlterOperatorStmt` | `ALTER OPERATOR ...` | Operator |
| **Text Search** | | |
| `AlterTSConfigurationStmt` | `ALTER TEXT SEARCH CONFIGURATION ...` | Text Search |
| `AlterTSDictionaryStmt` | `ALTER TEXT SEARCH DICTIONARY ...` | Text Search |
| **System** | | |
| `AlterSystemStmt` | `ALTER SYSTEM SET/RESET ...` | System |
| **Multi-Object** | | |
| `RenameStmt` | `ALTER ... RENAME TO ...` | Rename (55 alternatives) |
| `AlterObjectSchemaStmt` | `ALTER ... SET SCHEMA ...` | Schema Move (27 alternatives) |
| `AlterOwnerStmt` | `ALTER ... OWNER TO ...` | Ownership (24 alternatives) |
| `AlterObjectDependsStmt` | `ALTER ... DEPENDS ON EXTENSION ...` | Dependency (6 alternatives) |
| `CommentStmt` | `COMMENT ON ... IS ...` | Comment (18 alternatives) |
| `SecLabelStmt` | `SECURITY LABEL ON ... IS ...` | Security Label (10 alternatives) |
| **Drop (multi-object)** | | |
| `DropStmt` | `DROP TABLE/VIEW/INDEX/SEQUENCE/...` | Multi-object drop (12 alternatives) |
| `DropOwnedStmt` | `DROP OWNED BY ...` | Ownership |
| **Assertion (unimplemented in PG)** | | |
| `CreateAssertionStmt` | `CREATE ASSERTION ...` | Assertion (SQL standard, not implemented) |

---

## TCL (Transaction Control Language)

Grammar rule: `TransactionStmt` (12 alternatives)

| Mode | Behavior |
|------|----------|
| Ignored | Transaction statements have no effect (by design) |

**Design Decision:** ZTD manages virtual state through fixtures, making explicit transaction control unnecessary. Transaction statements are silently ignored rather than treated as unsupported.

**Ignored Statements (from grammar):**
- `BEGIN [WORK | TRANSACTION] [isolation_level] [READ ONLY | READ WRITE] [DEFERRABLE | NOT DEFERRABLE]`
- `START TRANSACTION [...]`
- `COMMIT [WORK | TRANSACTION] [AND [NO] CHAIN]`
- `ROLLBACK [WORK | TRANSACTION] [AND [NO] CHAIN]`
- `SAVEPOINT name`
- `RELEASE [SAVEPOINT] name`
- `ROLLBACK TO [SAVEPOINT] name`
- `PREPARE TRANSACTION 'gid'` - Two-phase commit prepare
- `COMMIT PREPARED 'gid'` - Two-phase commit
- `ROLLBACK PREPARED 'gid'` - Two-phase rollback

### Prepared Statements (PrepareStmt, ExecuteStmt, DeallocateStmt)

| Mode | Behavior |
|------|----------|
| Unsupported | Follows Unsupported SQL behavior |

**Grammar:** `PrepareStmt` (1 alternative), `ExecuteStmt` (3 alternatives), `DeallocateStmt` (4 alternatives).

**Note:** SQL-level `PREPARE`/`EXECUTE`/`DEALLOCATE` are server-side prepared statements. PDO-level prepared statements (`PDO::prepare()`) work normally since they are processed at the adapter level before ZTD rewriting.

---

## DCL (Data Control Language)

Grammar rules: `GrantStmt`, `RevokeStmt`, `GrantRoleStmt`, `RevokeRoleStmt`, `ReassignOwnedStmt`, `AlterDefaultPrivilegesStmt`

| Mode | Behavior |
|------|----------|
| Unsupported | Follows Unsupported SQL behavior (ignore/notice/exception) |

**Unsupported Statements (from grammar):**

| Grammar Rule | SQL Statement |
|-------------|---------------|
| `GrantStmt` | `GRANT privileges ON object TO grantee ...` |
| `RevokeStmt` | `REVOKE privileges ON object FROM grantee ...` |
| `GrantRoleStmt` | `GRANT role TO role ...` |
| `RevokeRoleStmt` | `REVOKE role FROM role ...` |
| `CreateRoleStmt` | `CREATE ROLE name ...` |
| `AlterRoleStmt` | `ALTER ROLE name ...` |
| `AlterRoleSetStmt` | `ALTER ROLE name SET/RESET ...` |
| `DropRoleStmt` | `DROP ROLE/USER/GROUP [IF EXISTS] name` (6 alternatives) |
| `CreateGroupStmt` | `CREATE GROUP name ...` |
| `AlterGroupStmt` | `ALTER GROUP name ADD/DROP USER ...` |
| `CreateUserStmt` | `CREATE USER name ...` |
| `AlterDefaultPrivilegesStmt` | `ALTER DEFAULT PRIVILEGES ...` |
| `ReassignOwnedStmt` | `REASSIGN OWNED BY ... TO ...` |

---

## Utility Statements

All utility statements from the grammar are unsupported by ZTD unless specifically noted.

### Information / Debugging

| Grammar Rule | SQL Statement | Mode | Notes |
|-------------|---------------|------|-------|
| `ExplainStmt` | `EXPLAIN [ANALYZE] [VERBOSE] ...` | Unsupported | Query plan analysis |
| `VariableShowStmt` | `SHOW name` / `SHOW ALL` / `SHOW TIME ZONE` / `SHOW TRANSACTION ISOLATION LEVEL` / `SHOW SESSION AUTHORIZATION` | Unsupported | Variable display |

### Session Management

| Grammar Rule | SQL Statement | Mode | Notes |
|-------------|---------------|------|-------|
| `VariableSetStmt` | `SET [SESSION\|LOCAL] name = value` | Unsupported | Variable setting |
| `VariableResetStmt` | `RESET name` / `RESET ALL` | Unsupported | Variable reset |
| `ConstraintsSetStmt` | `SET CONSTRAINTS ... DEFERRED/IMMEDIATE` | Unsupported | Constraint timing |
| `DiscardStmt` | `DISCARD ALL/PLANS/SEQUENCES/TEMP/TEMPORARY` | Unsupported | Session state cleanup (5 alternatives) |

### Maintenance

| Grammar Rule | SQL Statement | Mode | Notes |
|-------------|---------------|------|-------|
| `VacuumStmt` | `VACUUM [FULL] [FREEZE] [VERBOSE] [ANALYZE] ...` | Unsupported | Table maintenance |
| `AnalyzeStmt` | `ANALYZE [VERBOSE] table ...` | Unsupported | Statistics collection |
| `ClusterStmt` | `CLUSTER [VERBOSE] table [USING index]` | Unsupported | Physical reordering (5 alternatives) |
| `CheckPointStmt` | `CHECKPOINT` | Unsupported | WAL checkpoint |
| `LoadStmt` | `LOAD 'filename'` | Unsupported | Shared library loading |

### Cursor Management

| Grammar Rule | SQL Statement | Mode | Notes |
|-------------|---------------|------|-------|
| `DeclareCursorStmt` | `DECLARE cursor ... FOR SELECT ...` | Unsupported | Cursor declaration |
| `FetchStmt` | `FETCH/MOVE [direction] FROM/IN cursor` | Unsupported | Cursor navigation (16 fetch_args alternatives) |
| `ClosePortalStmt` | `CLOSE cursor` / `CLOSE ALL` | Unsupported | Cursor closing |

### Locking

| Grammar Rule | SQL Statement | Mode | Notes |
|-------------|---------------|------|-------|
| `LockStmt` | `LOCK [TABLE] name [IN mode MODE] [NOWAIT]` | Unsupported | Explicit table locking |

### LISTEN / NOTIFY

| Grammar Rule | SQL Statement | Mode | Notes |
|-------------|---------------|------|-------|
| `ListenStmt` | `LISTEN channel` | Unsupported | Asynchronous notification |
| `NotifyStmt` | `NOTIFY channel [, 'payload']` | Unsupported | Send notification |
| `UnlistenStmt` | `UNLISTEN channel` / `UNLISTEN *` | Unsupported | Stop listening |

---

## Complete Grammar Coverage

The following table maps all 124 named `stmt` alternatives to their ZTD handling mode:

| # | Grammar Rule | Mode | Category |
|---|-------------|------|----------|
| 1 | `SelectStmt` | **Rewrite** | DML |
| 2 | `InsertStmt` | **Rewrite** | DML |
| 3 | `UpdateStmt` | **Rewrite** | DML |
| 4 | `DeleteStmt` | **Rewrite** | DML |
| 5 | `TruncateStmt` | **Rewrite** | DML |
| 6 | `CreateStmt` | **Rewrite** | DDL (CREATE TABLE) |
| 7 | `CreateAsStmt` | **Rewrite** | DDL (CREATE TABLE AS) |
| 8 | `DropStmt` | **Rewrite** (TABLE only) | DDL (DROP) |
| 9 | `TransactionStmt` | **Ignored** | TCL |
| 10 | `MergeStmt` | Unsupported | DML |
| 11 | `CopyStmt` | Unsupported | DML |
| 12 | `CallStmt` | Unsupported | DML |
| 13 | `DoStmt` | Unsupported | DML |
| 14 | `AlterTableStmt` | Unsupported | DDL |
| 15 | `IndexStmt` | Unsupported | DDL |
| 16 | `ReindexStmt` | Unsupported | DDL |
| 17 | `ViewStmt` | Unsupported | DDL |
| 18 | `CreateMatViewStmt` | Unsupported | DDL |
| 19 | `RefreshMatViewStmt` | Unsupported | DDL |
| 20 | `CreateSchemaStmt` | Unsupported | DDL |
| 21 | `CreatedbStmt` | Unsupported | DDL |
| 22 | `AlterDatabaseStmt` | Unsupported | DDL |
| 23 | `AlterDatabaseSetStmt` | Unsupported | DDL |
| 24 | `DropdbStmt` | Unsupported | DDL |
| 25 | `CreateSeqStmt` | Unsupported | DDL |
| 26 | `AlterSeqStmt` | Unsupported | DDL |
| 27 | `CreateFunctionStmt` | Unsupported | DDL |
| 28 | `AlterFunctionStmt` | Unsupported | DDL |
| 29 | `RemoveFuncStmt` | Unsupported | DDL |
| 30 | `RemoveAggrStmt` | Unsupported | DDL |
| 31 | `RemoveOperStmt` | Unsupported | DDL |
| 32 | `DefineStmt` | Unsupported | DDL |
| 33 | `CreateDomainStmt` | Unsupported | DDL |
| 34 | `AlterDomainStmt` | Unsupported | DDL |
| 35 | `AlterEnumStmt` | Unsupported | DDL |
| 36 | `AlterTypeStmt` | Unsupported | DDL |
| 37 | `AlterCompositeTypeStmt` | Unsupported | DDL |
| 38 | `AlterCollationStmt` | Unsupported | DDL |
| 39 | `CreateTrigStmt` | Unsupported | DDL |
| 40 | `RuleStmt` | Unsupported | DDL |
| 41 | `CreateEventTrigStmt` | Unsupported | DDL |
| 42 | `AlterEventTrigStmt` | Unsupported | DDL |
| 43 | `CreateExtensionStmt` | Unsupported | DDL |
| 44 | `AlterExtensionStmt` | Unsupported | DDL |
| 45 | `AlterExtensionContentsStmt` | Unsupported | DDL |
| 46 | `CreateFdwStmt` | Unsupported | DDL |
| 47 | `AlterFdwStmt` | Unsupported | DDL |
| 48 | `CreateForeignServerStmt` | Unsupported | DDL |
| 49 | `AlterForeignServerStmt` | Unsupported | DDL |
| 50 | `CreateForeignTableStmt` | Unsupported | DDL |
| 51 | `CreateUserMappingStmt` | Unsupported | DDL |
| 52 | `AlterUserMappingStmt` | Unsupported | DDL |
| 53 | `DropUserMappingStmt` | Unsupported | DDL |
| 54 | `ImportForeignSchemaStmt` | Unsupported | DDL |
| 55 | `CreateTableSpaceStmt` | Unsupported | DDL |
| 56 | `AlterTblSpcStmt` | Unsupported | DDL |
| 57 | `DropTableSpaceStmt` | Unsupported | DDL |
| 58 | `CreatePolicyStmt` | Unsupported | DDL |
| 59 | `AlterPolicyStmt` | Unsupported | DDL |
| 60 | `CreateStatsStmt` | Unsupported | DDL |
| 61 | `AlterStatsStmt` | Unsupported | DDL |
| 62 | `CreatePublicationStmt` | Unsupported | DDL |
| 63 | `AlterPublicationStmt` | Unsupported | DDL |
| 64 | `CreateSubscriptionStmt` | Unsupported | DDL |
| 65 | `AlterSubscriptionStmt` | Unsupported | DDL |
| 66 | `DropSubscriptionStmt` | Unsupported | DDL |
| 67 | `CreateTransformStmt` | Unsupported | DDL |
| 68 | `DropTransformStmt` | Unsupported | DDL |
| 69 | `CreatePLangStmt` | Unsupported | DDL |
| 70 | `CreateConversionStmt` | Unsupported | DDL |
| 71 | `CreateAmStmt` | Unsupported | DDL |
| 72 | `CreateCastStmt` | Unsupported | DDL |
| 73 | `DropCastStmt` | Unsupported | DDL |
| 74 | `CreateOpClassStmt` | Unsupported | DDL |
| 75 | `CreateOpFamilyStmt` | Unsupported | DDL |
| 76 | `AlterOpFamilyStmt` | Unsupported | DDL |
| 77 | `DropOpClassStmt` | Unsupported | DDL |
| 78 | `DropOpFamilyStmt` | Unsupported | DDL |
| 79 | `AlterOperatorStmt` | Unsupported | DDL |
| 80 | `RenameStmt` | Unsupported | DDL |
| 81 | `AlterObjectSchemaStmt` | Unsupported | DDL |
| 82 | `AlterOwnerStmt` | Unsupported | DDL |
| 83 | `AlterObjectDependsStmt` | Unsupported | DDL |
| 84 | `CommentStmt` | Unsupported | DDL |
| 85 | `SecLabelStmt` | Unsupported | DDL |
| 86 | `DropOwnedStmt` | Unsupported | DDL |
| 87 | `CreateAssertionStmt` | Unsupported | DDL |
| 88 | `AlterSystemStmt` | Unsupported | DDL |
| 89 | `DropRoleStmt` | Unsupported | DCL |
| 90 | `CreateRoleStmt` | Unsupported | DCL |
| 91 | `AlterRoleStmt` | Unsupported | DCL |
| 92 | `AlterRoleSetStmt` | Unsupported | DCL |
| 93 | `CreateGroupStmt` | Unsupported | DCL |
| 94 | `AlterGroupStmt` | Unsupported | DCL |
| 95 | `CreateUserStmt` | Unsupported | DCL |
| 96 | `GrantStmt` | Unsupported | DCL |
| 97 | `RevokeStmt` | Unsupported | DCL |
| 98 | `GrantRoleStmt` | Unsupported | DCL |
| 99 | `RevokeRoleStmt` | Unsupported | DCL |
| 100 | `AlterDefaultPrivilegesStmt` | Unsupported | DCL |
| 101 | `ReassignOwnedStmt` | Unsupported | DCL |
| 102 | `PrepareStmt` | Unsupported | Utility |
| 103 | `ExecuteStmt` | Unsupported | Utility |
| 104 | `DeallocateStmt` | Unsupported | Utility |
| 105 | `ExplainStmt` | Unsupported | Utility |
| 106 | `VariableSetStmt` | Unsupported | Utility |
| 107 | `VariableResetStmt` | Unsupported | Utility |
| 108 | `VariableShowStmt` | Unsupported | Utility |
| 109 | `ConstraintsSetStmt` | Unsupported | Utility |
| 110 | `DiscardStmt` | Unsupported | Utility |
| 111 | `VacuumStmt` | Unsupported | Utility |
| 112 | `AnalyzeStmt` | Unsupported | Utility |
| 113 | `ClusterStmt` | Unsupported | Utility |
| 114 | `CheckPointStmt` | Unsupported | Utility |
| 115 | `LoadStmt` | Unsupported | Utility |
| 116 | `DeclareCursorStmt` | Unsupported | Utility |
| 117 | `FetchStmt` | Unsupported | Utility |
| 118 | `ClosePortalStmt` | Unsupported | Utility |
| 119 | `LockStmt` | Unsupported | Utility |
| 120 | `ListenStmt` | Unsupported | Utility |
| 121 | `NotifyStmt` | Unsupported | Utility |
| 122 | `UnlistenStmt` | Unsupported | Utility |
| 123 | `AlterTSConfigurationStmt` | Unsupported | DDL |
| 124 | `AlterTSDictionaryStmt` | Unsupported | DDL |

**Summary:** 8 Rewrite + 1 Ignored + 115 Unsupported = 124 total.

---

## Multiple Statements

| Mode | Behavior |
|------|----------|
| Rewrite | Process each statement sequentially via `rewriteMultiple()` |

**Supported:**
```sql
SELECT 1; SELECT 2
INSERT INTO t VALUES (1); UPDATE t SET x = 2
```

**Result:** `MultiRewritePlan` containing individual `RewritePlan` for each statement.

**Result Retrieval:** Use `nextRowset()` to iterate through result sets.

**Parser Note:** The PostgreSQL parser handles dollar-quoted strings (`$$...$$`, `$tag$...$tag$`), single-quoted strings (with `''` escaping), double-quoted identifiers, line comments (`--`), and block comments (`/* */`) when splitting statements by semicolons.

---

## CTE Shadowing Details (PostgreSQL-Specific)

### AS MATERIALIZED

PostgreSQL 12+ supports `AS MATERIALIZED` to prevent CTE inlining:

```sql
WITH "users" AS MATERIALIZED (
  SELECT ...
)
SELECT * FROM "users"
```

ZTD uses `AS MATERIALIZED` for all generated CTEs to ensure consistent behavior.

### Multi-Row CTEs via VALUES

For tables with multiple rows, PostgreSQL uses the `VALUES` clause:

```sql
WITH "users" AS MATERIALIZED (
  SELECT * FROM (VALUES
    (CAST(1 AS INTEGER), CAST('Alice' AS TEXT)),
    (CAST(2 AS INTEGER), CAST('Bob' AS TEXT))
  ) AS t("id", "name")
)
```

For single-row tables, a simple `SELECT` is used:

```sql
WITH "users" AS MATERIALIZED (SELECT CAST(1 AS INTEGER) AS "id", CAST('Alice' AS TEXT) AS "name")
```

### Empty CTEs

For tables with no fixture data:

```sql
WITH "users" AS MATERIALIZED (SELECT CAST(NULL AS INTEGER) AS "id", CAST(NULL AS TEXT) AS "name" WHERE FALSE)
```

### Identifier Quoting

PostgreSQL uses double-quote identifiers (`"identifier"`). Embedded double quotes are escaped by doubling (`""`).

```sql
-- Table name: my table
SELECT * FROM "my table"

-- Column name with quote: col"name
SELECT "col""name" FROM "users"
```

---

## Type Mapping

### ColumnTypeFamily to PostgreSQL CAST Types

| ColumnTypeFamily | PostgreSQL CAST Type |
|------------------|---------------------|
| `INTEGER` | `INTEGER` |
| `FLOAT` | `REAL` |
| `DOUBLE` | `DOUBLE PRECISION` |
| `DECIMAL` | `NUMERIC(p,s)` (preserves precision/scale) |
| `STRING` | `VARCHAR(n)` or `TEXT` |
| `TEXT` | `TEXT` |
| `BOOLEAN` | `BOOLEAN` |
| `DATE` | `DATE` |
| `TIME` | `TIME` |
| `DATETIME` | `TIMESTAMP` |
| `TIMESTAMP` | `TIMESTAMP` |
| `BINARY` | `BYTEA` |
| `JSON` | `JSONB` |
| `UNKNOWN` | Native type or `TEXT` fallback |

### Native Type to ColumnTypeFamily Mapping

| PostgreSQL Native Type | ColumnTypeFamily |
|----------------------|------------------|
| `INT`, `INT2`, `INT4`, `INT8`, `INTEGER`, `SMALLINT`, `BIGINT`, `SERIAL`, `SMALLSERIAL`, `BIGSERIAL` | `INTEGER` |
| `REAL`, `FLOAT4` | `FLOAT` |
| `DOUBLE PRECISION`, `FLOAT8` | `DOUBLE` |
| `DECIMAL`, `NUMERIC` | `DECIMAL` |
| `CHAR`, `CHARACTER`, `VARCHAR`, `CHARACTER VARYING`, `NAME` | `STRING` |
| `TEXT`, `CITEXT` | `TEXT` |
| `BOOLEAN`, `BOOL` | `BOOLEAN` |
| `DATE` | `DATE` |
| `TIME`, `TIMETZ`, `TIME WITH TIME ZONE`, `TIME WITHOUT TIME ZONE` | `TIME` |
| `TIMESTAMP`, `TIMESTAMPTZ`, `TIMESTAMP WITH TIME ZONE`, `TIMESTAMP WITHOUT TIME ZONE` | `TIMESTAMP` |
| `BYTEA` | `BINARY` |
| `JSON`, `JSONB` | `JSON` |

---

## Error Handling

### Error Classification

PostgreSQL errors are classified using SQLSTATE codes and error message patterns:

| SQLSTATE | Meaning | Classification |
|----------|---------|----------------|
| `42703` | `undefined_column` | Schema error |
| `42P01` | `undefined_table` | Schema error |
| `42P02` | `undefined_parameter` | Schema error |
| `42P10` | `invalid_column_reference` | Schema error |
| `42704` | `undefined_object` | Schema error |

**Message Pattern Matching:**
- `column "..." does not exist` - Schema error
- `relation "..." does not exist` - Schema error
- `table "..." does not exist` - Schema error

### Schema Errors

#### Non-existent Table

| Operation | Detection | Behavior |
|-----------|-----------|----------|
| SELECT FROM unknown_table | ZTD | `UnknownSchemaException` |
| INSERT INTO unknown_table | ZTD | `UnknownSchemaException` |
| UPDATE unknown_table | ZTD | `UnknownSchemaException` |
| DELETE FROM unknown_table | ZTD | `UnknownSchemaException` |
| DROP TABLE unknown_table | ZTD | `UnknownSchemaException` |
| ALTER TABLE unknown_table | ZTD | `UnknownSchemaException` |

#### Non-existent Column

| Operation | Detection | Behavior |
|-----------|-----------|----------|
| SELECT unknown_col FROM t | Database | PDOException from database |
| INSERT INTO t (unknown_col) | Database | PDOException from database |
| ALTER TABLE t actions | ZTD | `UnsupportedSqlException` (ALTER TABLE not supported) |

#### Table/Column Already Exists

| Operation | Detection | Behavior |
|-----------|-----------|----------|
| CREATE TABLE existing_table | ZTD | `UnsupportedSqlException` (Table already exists) |
| CREATE TABLE IF NOT EXISTS | ZTD | No action (table already exists) |

### Data Type Errors

ZTD applies CAST to all values in INSERT/UPDATE based on SchemaRegistry metadata. Type conversion errors are detected by the database.

**Example:**
```sql
-- Schema: users (id INTEGER, name TEXT, created_at TIMESTAMP)
-- Original
INSERT INTO users (id, name, created_at) VALUES ('123', 'Alice', '2024-01-01')

-- Result Select (with CAST)
SELECT
  CAST('123' AS INTEGER) AS "id",
  CAST('Alice' AS TEXT) AS "name",
  CAST('2024-01-01' AS TIMESTAMP) AS "created_at"
```

### Schema Reflection

PostgreSQL schema is reflected via `information_schema` queries:

- **Columns:** `information_schema.columns` (column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, is_nullable, column_default, udt_name)
- **Primary Keys:** `information_schema.table_constraints` + `information_schema.key_column_usage` (constraint_type = 'PRIMARY KEY')
- **Unique Constraints:** `information_schema.table_constraints` + `information_schema.key_column_usage` (constraint_type = 'UNIQUE')
- **Tables:** `information_schema.tables` (table_schema = 'public', table_type = 'BASE TABLE')

---

## Unsupported SQL Handling

When an unsupported SQL statement is executed, behavior depends on configuration. Each statement pattern can be configured with a specific behavior.

### Behavior Modes

| Mode | Description |
|------|-------------|
| `ignore` | Silently ignore, return empty result |
| `notice` | Log warning, continue with empty result |
| `exception` | Throw `UnsupportedSqlException` |
| `passthrough` | Execute against real database as-is |

### Per-Statement Configuration

| Pattern | Behavior | Reason |
|---------|----------|--------|
| `SET search_path` | `passthrough` | Affects schema resolution |
| `SET client_encoding` | `passthrough` | Affects connection encoding |
| `EXPLAIN ...` | `passthrough` or `ignore` | Query plan analysis |
| `CREATE INDEX` | `ignore` | Index management not needed in tests |
| `ALTER TABLE` | `exception` | Not yet supported |
| Default | `ignore` | Fallback for unspecified patterns |

---

## Limitations

1. **ALTER TABLE** - Not yet supported for PostgreSQL. Follows Unsupported SQL behavior.
2. **MERGE** - Not supported. Complex multi-branch conditional mutation is beyond current scope.
3. **RETURNING clause** - Stripped from INSERT/UPDATE/DELETE. ZTD uses Result Select Query approach instead.
4. **Dollar-quoted strings** - Supported in statement splitting but not in all parsing contexts.
5. **Schema-qualified names** - Schema prefix is stripped (e.g., `public.users` becomes `users`).
6. **Inheritance (ONLY)** - The `ONLY` keyword is accepted but inheritance semantics are not simulated.
7. **Stored Procedures / Functions** - Not supported (opaque execution).
8. **Array types** - Recognized in schema parsing but not fully handled in CAST rendering.
9. **CHECK constraints** - Not evaluated by ZTD.
10. **Triggers / Rules** - Not simulated.
11. **WHERE CURRENT OF** - Cursor-based UPDATE/DELETE is not supported.
12. **COPY** - Bulk copy operations require file system access.

---

## Design Principles

1. **Zero Physical Impact** - All operations are virtual; physical tables are never modified
2. **SQL as Pure Functions** - Input (fixtures) and output (query results) only, no side effects
3. **Real Database Engine** - Uses actual database for SQL parsing, type inference, and query execution
4. **CTE-Based Isolation** - Tables are shadowed with CTEs using `AS MATERIALIZED`, not mocked
5. **Simplicity** - Complex features (stored procedures, triggers, views) are out of scope
6. **Type Conversion via CAST** - Schema metadata used to apply appropriate type casts
7. **Result Select over RETURNING** - Consistent cross-platform approach; does not rely on PostgreSQL-specific `RETURNING`
