<?php

declare(strict_types=1);

namespace Container;

/**
 * Defines the MySQL 5.6.51 server used by database consumers.
 *
 * @example Inspect the pinned server image without starting Docker
 *     $container = new \Container\MySql56Container();
 *     assert($container->image() === 'mysql:5.6.51');
 */
final class MySql56Container extends MySqlContainer
{
    protected static $IMAGE = 'mysql:5.6.51';
}
