<?php

declare(strict_types=1);

namespace Container;

use RuntimeException;
use Testcontainers\Containers\ContainerInstance;
use Testcontainers\Containers\GenericContainer\GenericContainer;
use Testcontainers\Containers\WaitStrategy\LogMessageWaitStrategy;
use Testcontainers\Containers\WaitStrategy\WaitStrategy;

/**
 * Defines a reusable PostgreSQL server with a disposable `test` database.
 *
 * Each version extends it and sets only `$IMAGE`. The server is ready once it logs that
 * it accepts connections; the user is `test` / `test`.
 */
abstract class PostgreSqlContainer extends GenericContainer
{
    protected static $EXPOSED_PORTS = [5432];

    protected static $ENVIRONMENTS = [
        'POSTGRES_USER' => 'test',
        'POSTGRES_PASSWORD' => 'test',
        'POSTGRES_DB' => 'test',
    ];

    protected static $REUSE_MODE = 'reuse';

    protected static $STARTUP_TIMEOUT = 300;

    protected static $STARTUP_CONFLICT_RETRY_ATTEMPTS = 10;

    protected static $AUTO_REMOVE_ON_EXIT = true;

    /**
     * Attaches the server endpoint once the container has started.
     *
     * The host is given as `127.0.0.1` instead of `localhost` so that clients connect over TCP.
     *
     * @param ContainerInstance $instance Started container instance.
     * @throws RuntimeException If the server port is not mapped to the host.
     */
    public function afterStart(ContainerInstance $instance): void
    {
        $port = $instance->getMappedPort(5432) ?? throw new RuntimeException('Port 5432 is not mapped to the host.');
        $host = str_replace('localhost', '127.0.0.1', $instance->getHost());
        $instance->setData(new Endpoint('pgsql', $host, $port, 'test', 'test', 'test'));
    }

    /**
     * Waits until the server logs that it accepts connections.
     *
     * @param ContainerInstance $instance Started container instance.
     * @return WaitStrategy Log message wait strategy.
     */
    protected function waitStrategy($instance): WaitStrategy
    {
        return (new LogMessageWaitStrategy())
            ->withPattern('\\[1\\].*database system is ready to accept connections')
            ->withTimeoutSeconds(120);
    }
}
