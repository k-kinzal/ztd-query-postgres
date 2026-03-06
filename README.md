# ZTD Query PostgreSQL

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![PHP Version](https://img.shields.io/badge/PHP-8.1%2B-blue.svg)](https://www.php.net/)

PostgreSQL platform support for [ZTD Query PHP](https://github.com/k-kinzal/ztd-query-core). Provides SQL parsing, classification, rewriting, and schema management for PostgreSQL.

## Overview

This package implements the PostgreSQL-specific logic for ZTD (Zero Table Dependency) query transformation. It handles:

- **SQL Parsing** - Parse PostgreSQL statements using a built-in regex-based parser
- **Query Classification** - Classify queries as READ, WRITE_SIMULATED, or DDL_SIMULATED
- **CTE Rewriting** - Transform SELECT queries to use CTE-shadowed fixture data
- **Result Select Query** - Convert INSERT/UPDATE/DELETE into SELECT queries returning affected rows
- **Schema Management** - Reflect and track PostgreSQL table definitions via `information_schema` queries
- **Error Classification** - Identify PostgreSQL-specific error codes for unknown schema detection

This package is used internally by the [PDO adapter](https://github.com/k-kinzal/ztd-query-pdo-adapter), but can also be used directly for custom adapter implementations.

## Requirements

- PHP 8.1 or higher
- [k-kinzal/ztd-query-php](https://github.com/k-kinzal/ztd-query-core) (core)

## Installation

```bash
composer require k-kinzal/ztd-query-postgres
```

## Usage

### Creating a PostgreSQL Session

`PgSqlSessionFactory` is the main entry point. It creates a fully configured `Session` instance for PostgreSQL:

```php
use ZtdQuery\Config\ZtdConfig;
use ZtdQuery\Platform\Postgres\PgSqlSessionFactory;

// $connection implements ZtdQuery\Connection\ConnectionInterface
$session = PgSqlSessionFactory::create($connection, ZtdConfig::default());
```

The factory automatically:
1. Reflects the database schema via `information_schema` queries
2. Sets up the SQL parser, query guard, and all transformers
3. Configures the shadow store for virtual write tracking

### Query Classification

`PgSqlQueryGuard` classifies SQL statements into query kinds:

```php
use ZtdQuery\Platform\Postgres\PgSqlQueryGuard;
use ZtdQuery\Platform\Postgres\PgSqlParser;
use ZtdQuery\Rewrite\QueryKind;

$parser = new PgSqlParser();
$guard = new PgSqlQueryGuard($parser);

$guard->classify('SELECT * FROM users');
// => QueryKind::READ

$guard->classify('INSERT INTO users (name) VALUES (\'Alice\')');
// => QueryKind::WRITE_SIMULATED

$guard->classify('CREATE TABLE logs (id INT)');
// => QueryKind::DDL_SIMULATED

$guard->classify('BEGIN');
// => null (unsupported)
```

### SQL Rewriting

`PgSqlRewriter` transforms SQL statements for ZTD execution:

```php
use ZtdQuery\Platform\Postgres\PgSqlRewriter;

// Rewrite a single statement
$plan = $rewriter->rewrite('SELECT email FROM users WHERE id = 1');
// $plan->sql() returns the CTE-shadowed query
// $plan->kind() returns the QueryKind

// Rewrite multiple statements (e.g., multi-query)
$plans = $rewriter->rewriteMultiple('SELECT 1; SELECT 2');
```

### Error Classification

`PgSqlErrorClassifier` identifies PostgreSQL error codes related to unknown schemas:

```php
use ZtdQuery\Platform\Postgres\PgSqlErrorClassifier;

$classifier = new PgSqlErrorClassifier();

$classifier->isUnknownSchemaError('42P01'); // true (Undefined table)
$classifier->isUnknownSchemaError('42703'); // true (Undefined column)
$classifier->isUnknownSchemaError('42601'); // false (Syntax error)
```

## Architecture

```
PgSqlSessionFactory
    |
    +-- PgSqlParser (regex-based SQL parsing)
    +-- PgSqlQueryGuard (query classification)
    +-- PgSqlSchemaReflector (database schema reflection via information_schema)
    +-- PgSqlSchemaParser (CREATE TABLE parsing)
    +-- PgSqlRewriter (query rewriting orchestrator)
    |       +-- PgSqlTransformer
    |       |       +-- SelectTransformer (CTE injection)
    |       |       +-- InsertTransformer (INSERT -> SELECT)
    |       |       +-- UpdateTransformer (UPDATE -> SELECT)
    |       |       +-- DeleteTransformer (DELETE -> SELECT)
    |       +-- PgSqlMutationResolver (virtual DDL tracking)
    +-- PgSqlErrorClassifier (error code classification)
```

## SQL Support

### Fully Supported

- **SELECT**: All clauses including JOIN, GROUP BY, HAVING, ORDER BY, LIMIT, OFFSET, FETCH FIRST, UNION, INTERSECT, EXCEPT, subqueries, CTEs, window functions, DISTINCT ON, FOR UPDATE/SHARE
- **INSERT**: VALUES, SELECT, ON CONFLICT (upsert)
- **UPDATE**: Single-table with WHERE
- **DELETE**: Single-table with WHERE
- **TRUNCATE**
- **DDL**: CREATE TABLE, ALTER TABLE, DROP TABLE (virtual schema)
- **WITH**: CTE and recursive CTE

### Unsupported

- MERGE
- Stored procedures, triggers, functions, views
- Database/schema operations
- User/permission management
- Server operations (VACUUM, ANALYZE, REINDEX, etc.)

## Development

```bash
# Run tests
composer test

# Run unit tests
composer test:unit

# Run linter (PHP-CS-Fixer + PHPStan level max)
composer lint

# Run fuzz tests
composer fuzz:robustness
composer fuzz:robustness:classify
composer fuzz:robustness:rewrite

# Fix code style
composer format
```

## License

MIT License. See [LICENSE](LICENSE) for details.
