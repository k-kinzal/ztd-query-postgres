<?php

declare(strict_types=1);

namespace Fuzz\Target;

use Error;
use PDO;
use PDOException;
use PDOStatement;
use Throwable;
use ZtdQuery\Config\UnknownSchemaBehavior;
use ZtdQuery\Config\UnsupportedSqlBehavior;
use ZtdQuery\Config\ZtdConfig;
use ZtdQuery\Connection\ConnectionInterface;
use ZtdQuery\Connection\Exception\DatabaseException;
use ZtdQuery\Connection\ResultColumn;
use ZtdQuery\Connection\StatementInterface;
use ZtdQuery\Exception\DuplicateKeyException;
use ZtdQuery\Exception\NotNullViolationException;
use ZtdQuery\Exception\UnknownSchemaException;
use ZtdQuery\Platform\Postgres\PgSqlPlatform;
use ZtdQuery\Platform\ResultColumnTypeResolver;
use ZtdQuery\QueryExecutor;
use ZtdQuery\Rewrite\QueryKind;

/**
 * Compares native SQL with core QueryExecutor plus Platform, independently of adapters.
 * Checks rows (including multiplicity and NULL), affected counts, rejection reasons,
 * subsequent table reads, and physical isolation, including on rejected statements.
 * Unordered row bags are compared; result ordering is outside this target's contract.
 */
final class BehaviorTarget implements ConnectionInterface
{
    private PDO $physical;

    /**
     * Connect only to the disposable server owned by the entrypoint.
     */
    public function __construct(private readonly string $dsn)
    {
    }

    /**
     * Reset a private schema, executing generated SQL without superuser privileges.
     */
    public function database(string $side): PDO
    {
        $name = 'ztd_fuzz_' . getmypid();
        $options = [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION, PDO::ATTR_EMULATE_PREPARES => false];
        try {
            $root = new PDO($this->dsn, 'test', 'test', $options);
            $root->exec("DROP SCHEMA IF EXISTS $name CASCADE");
            $root->exec("DROP ROLE IF EXISTS $name");
            $root->exec("CREATE ROLE $name LOGIN PASSWORD '$name'");
            $root->exec("CREATE SCHEMA $name AUTHORIZATION $name");
            $pdo = new PDO($this->dsn, $name, $name, $options);
            $pdo->exec("SET search_path TO $name");
            $pdo->exec('SET statement_timeout = 2000');
            $pdo->exec('SET lock_timeout = 2000');
            $pdo->exec('CREATE TABLE items (id INTEGER PRIMARY KEY NOT NULL, value INTEGER, label VARCHAR(80))');
            if ($side === 'physical') {
                $pdo->exec("INSERT INTO items VALUES (9999, 9999, 'physical-only')");
            }
            return $pdo;
        } catch (PDOException $failure) {
            fwrite(STDERR, "PostgreSQL setup failed: {$failure->getMessage()}\n");
            exit(2);
        }
    }

    /**
     * Run a grammar statement against equal fixtures and compare observable effects.
     * Native execution is repeated on a fresh database to identify volatile SQL.
     * All three runs reuse the same catalog and user names, avoiding environment mismatches.
     * No SQL exception is accepted merely because both executions threw something.
     *
     * @throws Error When ZTD differs from the native server
     * @throws PDOException When reading a database fails
     * @throws DatabaseException When fixture setup through ZTD fails
     */
    public function verify(string $sql, string $input): void
    {

        unset($this->physical);
        $fixtures = "INSERT INTO items VALUES (1, -2, 'first'), (2, 0, NULL), (3, 7, 'O''Brien')";
        $native = $this->database('reference');
        $native->exec($fixtures);
        $native->exec('UPDATE items SET value = value WHERE 0 = 1');
        $expected = $this->native($native, $sql);
        $state = $this->tables($native);
        $native = null;
        $native = $this->database('reference');
        $native->exec($fixtures);
        $native->exec('UPDATE items SET value = value WHERE 0 = 1');
        $stable = $this->native($native, $sql) === $expected && $this->tables($native) === $state;
        $native = null;
        $this->physical = $this->database('physical');
        $before = $this->snapshot($this->physical);
        $context = 'Input (hex): ' . bin2hex($input) . "\nSQL: {$sql}";
        try {
            $executor = new QueryExecutor($this, new PgSqlPlatform(), new ZtdConfig(UnsupportedSqlBehavior::Exception, UnknownSchemaBehavior::Exception));
            $this->execute($executor, $fixtures);
            $this->physical->exec('UPDATE items SET value = value WHERE 0 = 1');
            $actual = $this->virtual($executor, $sql);
            if (!$stable) {
                return;
            }
            if ($expected !== $actual) {
                throw new Error($context . "\nNative and ZTD outcomes differ.\nExpected: " . var_export($expected, true) . "\nActual: " . var_export($actual, true));
            }
            foreach (array_unique(['items', ...array_keys($state)]) as $table) {
                $definition = $executor->session()->tableDefinition($table);
                $observed = $definition === null ? null : [
                    'columns' => $definition->columns,
                    'rows' => $this->virtual($executor, 'SELECT * FROM ' . $this->quote($table)),
                ];
                if (($state[$table] ?? null) !== $observed) {
                    throw new Error($context . "\nVirtual table differs after the statement: " . $table . "\nExpected: " . var_export($state[$table] ?? null, true) . "\nActual: " . var_export($observed, true));
                }
            }
        } finally {

            if ($before !== $this->snapshot($this->physical)) {
                throw new Error("The physical database changed.\n" . $context);
            }
        }
    }

    /**
     * Execute through core, including transaction statements and returning writes.
     * @return array<mixed>
     * @throws PDOException When native result-select SQL fails
     * @throws DatabaseException When ZTD rejects a statement
     * @throws Error When ZTD silently skips a statement
     */
    public function execute(QueryExecutor $executor, string $sql): array
    {
        $transaction = $executor->transactionStatement($sql);
        if ($transaction !== null) {
            $executor->session()->applyTransactionStatement($transaction);
            return ['ok', 0];
        }
        $plan = $executor->rewrite($sql);
        if ($plan->kind() === QueryKind::SKIPPED) {
            throw new Error('ZTD silently skipped: ' . $sql);
        }
        $result = $executor->processExecutedStatement($plan, $this->query($plan->sql()));
        return ['ok', $result->hasResultSet() ? $this->rows($result->fetchAll()) : $result->rowCount()];
    }

    /**
     * Capture declared database rejections, leaving programming failures to PHP-Fuzzer.
     * @return array<mixed>
     * @throws Error When the session silently skips SQL
     */
    public function virtual(QueryExecutor $executor, string $sql): array
    {
        try {
            return $this->execute($executor, $sql);
        } catch (PDOException|DatabaseException $failure) {
            return ['error', $this->rejection($failure)];
        }
    }

    /**
     * Use the native server as the outcome oracle, including for invalid statements.
     * @return array<mixed>
     */
    public function native(PDO $pdo, string $sql): array
    {
        try {
            $statement = $pdo->query($sql);
            if ($statement === false) {
                fwrite(STDERR, "PDO::query failed without an exception.\n");
                exit(2);
            }
            $answer = ['ok', $statement->columnCount() > 0 ? $this->rows($statement->fetchAll(PDO::FETCH_ASSOC)) : $statement->rowCount()];
            $statement->closeCursor();
            return $answer;
        } catch (PDOException $failure) {
            return ['error', $this->rejection($failure)];
        }
    }

    /**
     * Compare narrowly identified domain failures; every other native error keeps its identity.
     * Unknown ZTD rejections retain their exception class and message and cannot match native errors.
     */
    public function rejection(Throwable $failure): string
    {
        for ($cause = $failure; $cause !== null; $cause = $cause->getPrevious()) {
            if ($cause instanceof DuplicateKeyException) {
                return 'unique';
            }
            if ($cause instanceof NotNullViolationException) {
                return 'not-null';
            }
            if ($cause instanceof UnknownSchemaException) {
                return 'missing-table';
            }
            if ($cause instanceof PDOException) {
                $state = $cause->errorInfo[0] ?? '';
                $code = $cause->errorInfo[1] ?? 0;
                $message = $cause->errorInfo[2] ?? $cause->getMessage();
                $state = is_string($state) ? $state : '';
                $message = is_string($message) ? $message : $cause->getMessage();
                if (str_starts_with($state, '08') || str_starts_with($state, '57P0') || in_array($code, [2002, 2006, 2013], true)) {
                    fwrite(STDERR, 'Database connection failed: ' . $message . "\n");
                    exit(2);
                }
                if ($state === '23505' || $code === 1062 || str_contains($message, 'UNIQUE constraint failed:')) {
                    return 'unique';
                }
                if ($state === '23502' || $code === 1048 || str_contains($message, 'NOT NULL constraint failed:')) {
                    return 'not-null';
                }
                if ($state === '42P01' || $code === 1146 || str_starts_with($message, 'no such table:')) {
                    return 'missing-table';
                }
                $message = preg_replace('/\n(?:LINE [0-9]+:|POSITION:).*$/s', '', $message) ?? $message;
                return $state . ':' . (is_scalar($code) ? (string) $code : '') . ':' . $message;
            }
        }
        return $failure::class . ':' . $failure->getMessage();
    }

    /**
     * Preserve driver values, column names, NULLs and duplicates, ignoring only row order.
     * @param array<mixed> $rows
     * @return list<string>
     */
    public function rows(array $rows): array
    {
        $rows = array_map(serialize(...), $rows);
        sort($rows, SORT_STRING);
        return $rows;
    }

    /**
     * Inspect every current user table, including tables created by the generated statement.
     * @return array<string, array{columns: list<string>, rows: array<mixed>}>
     * @throws PDOException When catalog inspection fails
     */
    public function tables(PDO $pdo): array
    {
        $tables = [];
        $catalog = $pdo->query('SELECT tablename FROM pg_tables WHERE schemaname = current_schema() ORDER BY tablename');
        if ($catalog === false) {
            fwrite(STDERR, "Cannot inspect the database catalog.\n");
            exit(2);
        }
        foreach ($catalog->fetchAll(PDO::FETCH_COLUMN) as $name) {
            if (!is_string($name)) {
                continue;
            }
            $statement = $pdo->query('SELECT * FROM ' . $this->quote($name));
            if ($statement === false) {
                fwrite(STDERR, "Cannot inspect table rows.\n");
                exit(2);
            }
            $columns = [];
            for ($index = 0; $index < $statement->columnCount(); ++$index) {
                $metadata = $statement->getColumnMeta($index);
                if ($metadata === false) {
                    fwrite(STDERR, "Cannot inspect result metadata.\n");
                    exit(2);
                }
                $columns[] = $metadata['name'];
            }
            $tables[$name] = ['columns' => $columns, 'rows' => ['ok', $this->rows($statement->fetchAll(PDO::FETCH_ASSOC))]];
        }
        ksort($tables);
        return $tables;
    }

    /**
     * Include catalog definitions so that a physical DDL change is also a finding.
     * @return array<mixed>
     * @throws PDOException When snapshot inspection fails
     */
    public function snapshot(PDO $pdo): array
    {
        return [$this->tables($pdo),
            $this->native($pdo, 'SELECT table_name, column_name, ordinal_position, data_type, is_nullable, column_default FROM information_schema.columns WHERE table_schema = current_schema()'),
            $this->native($pdo, 'SELECT tablename, indexname, indexdef FROM pg_indexes WHERE schemaname = current_schema()'),
            $this->native($pdo, 'SELECT viewname, definition FROM pg_views WHERE schemaname = current_schema()'),
            $this->native($pdo, 'SELECT conname, pg_get_constraintdef(oid) FROM pg_constraint WHERE connamespace = current_schema()::regnamespace'),
            $this->native($pdo, 'SELECT trigger_name, action_statement FROM information_schema.triggers WHERE trigger_schema = current_schema()'),
        ];
    }

    /**
     * Quote catalog identifiers without interpreting generated SQL.
     */
    public function quote(string $name): string
    {
        return '"' . str_replace('"', '"' . '"', $name) . '"';
    }

    /**
     * Minimal native bridge for the platform contract; no production adapter is involved.
     * @throws PDOException When result-select SQL fails on the server
     * @throws Error When the driver silently fails
     */
    public function query(string $sql): StatementInterface
    {
        $statement = $this->physical->query($sql);
        if ($statement === false) {
            throw new Error('Native result-select execution silently failed.');
        }
        return new class ($statement) implements StatementInterface {
            /**
             * Wrap the native result.
             */
            public function __construct(private readonly PDOStatement $statement)
            {
            }

            /**
             * Execute the native statement.
             */
            public function execute(?array $params = null): bool
            {
                return $this->statement->execute($params);
            }

            /**
             * Fetch native rows without coercing driver values.
             * @throws Error When a native row has an unexpected shape
             */
            public function fetchAll(): array
            {
                $rows = [];
                foreach ($this->statement->fetchAll(PDO::FETCH_ASSOC) as $row) {
                    if (!is_array($row)) {
                        throw new Error('The driver did not return an associative row.');
                    }
                    $values = [];
                    foreach ($row as $column => $value) {
                        $values[(string) $column] = $value;
                    }
                    $rows[] = $values;
                }
                return $rows;
            }

            /**
             * Resolve native column metadata using the platform contract.
             * @throws Error When metadata is unavailable
             */
            public function resultColumns(ResultColumnTypeResolver $typeResolver): array
            {
                $columns = [];
                for ($index = 0; $index < $this->statement->columnCount(); ++$index) {
                    $metadata = $this->statement->getColumnMeta($index);
                    if ($metadata === false) {
                        throw new Error('The driver omitted result metadata.');
                    }
                    $columns[] = new ResultColumn($metadata['name'], $typeResolver->resolve($metadata));
                }
                return $columns;
            }

            /**
             * Return the native affected count.
             */
            public function rowCount(): int
            {
                return $this->statement->rowCount();
            }
        };
    }
}
