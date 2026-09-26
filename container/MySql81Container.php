<?php

declare(strict_types=1);

namespace Container;

/**
 * Defines the MySQL 8.1.0 server used by database consumers.
 *
 * @example Inspect the pinned server image without starting Docker
 *     $container = new \Container\MySql81Container();
 *     assert($container->image() === 'mysql:8.1.0');
 */
final class MySql81Container extends MySqlContainer
{
    protected static $IMAGE = 'mysql:8.1.0';
}
