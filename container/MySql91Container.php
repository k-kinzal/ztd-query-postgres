<?php

declare(strict_types=1);

namespace Container;

/**
 * Defines the MySQL 9.1.0 server used by database consumers.
 *
 * @example Inspect the pinned server image without starting Docker
 *     $container = new \Container\MySql91Container();
 *     assert($container->image() === 'container-registry.oracle.com/mysql/community-server:9.1.0');
 */
final class MySql91Container extends MySqlContainer
{
    protected static $IMAGE = 'container-registry.oracle.com/mysql/community-server:9.1.0';
}
