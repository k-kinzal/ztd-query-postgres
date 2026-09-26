<?php

declare(strict_types=1);

namespace Container;

/**
 * Defines the MySQL 8.3.0 server used by database consumers.
 *
 * @example Inspect the pinned server image without starting Docker
 *     $container = new \Container\MySql83Container();
 *     assert($container->image() === 'mysql:8.3.0');
 */
final class MySql83Container extends MySqlContainer
{
    protected static $IMAGE = 'mysql:8.3.0';
}
