<?php

declare(strict_types=1);

namespace Container;

/**
 * Defines the MySQL 8.2.0 server used by database consumers.
 *
 * @example Inspect the pinned server image without starting Docker
 *     $container = new \Container\MySql82Container();
 *     assert($container->image() === 'mysql:8.2.0');
 */
final class MySql82Container extends MySqlContainer
{
    protected static $IMAGE = 'mysql:8.2.0';
}
