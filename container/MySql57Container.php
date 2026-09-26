<?php

declare(strict_types=1);

namespace Container;

/**
 * Defines the MySQL 5.7.44 server used by database consumers.
 *
 * @example Inspect the pinned server image without starting Docker
 *     $container = new \Container\MySql57Container();
 *     assert($container->image() === 'mysql:5.7.44');
 */
final class MySql57Container extends MySqlContainer
{
    protected static $IMAGE = 'mysql:5.7.44';
}
