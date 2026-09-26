<?php

declare(strict_types=1);

namespace Container;

/**
 * Defines the PostgreSQL 17.2 server used by database consumers.
 *
 * @example Inspect the pinned server image without starting Docker
 *     $container = new \Container\PostgreSql17Container();
 *     assert($container->image() === 'postgres:17.2');
 */
final class PostgreSql17Container extends PostgreSqlContainer
{
    protected static $IMAGE = 'postgres:17.2';
}
