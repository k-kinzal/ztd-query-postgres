<?php

declare(strict_types=1);

namespace Container;

/**
 * Defines the MySQL 8.4.7 server used by database consumers.
 *
 * @example Inspect the pinned server image without starting Docker
 *     $container = new \Container\MySql84Container();
 *     assert($container->image() === 'container-registry.oracle.com/mysql/community-server:8.4.7');
 */
final class MySql84Container extends MySqlContainer
{
    protected static $IMAGE = 'container-registry.oracle.com/mysql/community-server:8.4.7';
}
