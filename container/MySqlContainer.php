<?php

declare(strict_types=1);

namespace Container;

use RuntimeException;
use Testcontainers\Containers\ContainerInstance;
use Testcontainers\Containers\GenericContainer\GenericContainer;
use Testcontainers\Containers\WaitStrategy\PDO\MySQLDSN;
use Testcontainers\Containers\WaitStrategy\PDO\PDOConnectWaitStrategy;
use Testcontainers\Containers\WaitStrategy\WaitStrategy;

/**
 * Defines a reusable MySQL server with a disposable `test` database.
 *
 * Each version extends it and sets only `$IMAGE`. The server runs on tmpfs and skips
 * loading the time zone tables to start fast; it is ready once PDO can connect as
 * `root` / `root`.
 */
abstract class MySqlContainer extends GenericContainer
{
    protected static $EXPOSED_PORTS = [3306];

    protected static $MOUNTS = ['type=tmpfs,destination=/var/lib/mysql'];

    protected static $ENVIRONMENTS = [
        'MYSQL_ROOT_PASSWORD' => 'root',
        'MYSQL_ROOT_HOST' => '%',
        'MYSQL_DATABASE' => 'test',
        'MYSQL_INITDB_SKIP_TZINFO' => '1',
    ];

    protected static $REUSE_MODE = 'reuse';

    protected static $STARTUP_TIMEOUT = 300;

    protected static $STARTUP_CONFLICT_RETRY_ATTEMPTS = 10;

    protected static $AUTO_REMOVE_ON_EXIT = true;

    /**
     * Returns the SQL grammar identifier matching the server version.
     *
     * @return string Grammar identifier, `mysql-` followed by the image tag.
     */
    public static function getGrammarVersion(): string
    {
        $image = (string) static::$IMAGE;

        return 'mysql-' . substr($image, (int) strrpos($image, ':') + 1);
    }

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
        $port = $instance->getMappedPort(3306) ?? throw new RuntimeException('Port 3306 is not mapped to the host.');
        $host = str_replace('localhost', '127.0.0.1', $instance->getHost());
        $instance->setData(new Endpoint('mysql', $host, $port, 'test', 'root', 'root'));
    }

    /**
     * Waits until PDO can connect to the `test` database.
     *
     * @param ContainerInstance $instance Started container instance.
     * @return WaitStrategy PDO connection wait strategy.
     */
    protected function waitStrategy($instance): WaitStrategy
    {
        return (new PDOConnectWaitStrategy())
            ->withDsn((new MySQLDSN())->withDbname('test')->withCharset('utf8mb4'))
            ->withUsername('root')
            ->withPassword('root')
            ->withTimeoutSeconds(120)
            ->withRetryInterval(250000);
    }
}
