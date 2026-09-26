<?php

declare(strict_types=1);

namespace Container;

/**
 * Describes how to connect to the database server of a started container.
 *
 * A database container attaches it to its instance after startup, so it is read with
 * `Testcontainers::run(...)->getData(Endpoint::class)`.
 *
 * @example Build the PDO data source name of a MySQL server
 *     $endpoint = new \Container\Endpoint('mysql', '127.0.0.1', 33060, 'test', 'root', 'root');
 *     assert($endpoint->dsn() === 'mysql:host=127.0.0.1;port=33060;dbname=test;charset=utf8mb4');
 * @example Build the PDO data source name of a PostgreSQL server
 *     $endpoint = new \Container\Endpoint('pgsql', '127.0.0.1', 54320, 'test', 'test', 'test');
 *     assert($endpoint->dsn() === 'pgsql:host=127.0.0.1;port=54320;dbname=test');
 */
final class Endpoint
{
    /**
     * @param string $driver PDO driver name, `mysql` or `pgsql`.
     * @param string $host Host that reaches the server over TCP.
     * @param int $port Host port mapped to the server port.
     * @param string $database Database created at startup.
     * @param string $username User that owns the database.
     * @param string $password Password of the user.
     */
    public function __construct(
        public readonly string $driver,
        public readonly string $host,
        public readonly int $port,
        public readonly string $database,
        public readonly string $username,
        public readonly string $password,
    ) {
    }

    /**
     * Returns the PDO data source name of the server.
     *
     * @return string Data source name; MySQL connections use `utf8mb4`.
     */
    public function dsn(): string
    {
        $dsn = sprintf('%s:host=%s;port=%d;dbname=%s', $this->driver, $this->host, $this->port, $this->database);

        return $this->driver === 'mysql' ? $dsn . ';charset=utf8mb4' : $dsn;
    }
}
