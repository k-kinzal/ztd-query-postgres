<?php

declare(strict_types=1);

namespace Container;

/**
 * Defines the MySQL 9.0.1 server used by database consumers.
 *
 * @example Inspect the pinned server image without starting Docker
 *     $container = new \Container\MySql90Container();
 *     assert($container->image() === 'container-registry.oracle.com/mysql/community-server:9.0.1');
 */
final class MySql90Container extends MySqlContainer
{
    protected static $IMAGE = 'container-registry.oracle.com/mysql/community-server:9.0.1';
}
