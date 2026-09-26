<?php

declare(strict_types=1);

namespace Container;

/**
 * Defines the MySQL 8.0.44 server used by database consumers.
 *
 * @example Inspect the pinned server image without starting Docker
 *     $container = new \Container\MySql80Container();
 *     assert($container->image() === 'mysql:8.0.44');
 */
final class MySql80Container extends MySqlContainer
{
    protected static $IMAGE = 'mysql:8.0.44';
}
