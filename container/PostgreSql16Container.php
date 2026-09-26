<?php

declare(strict_types=1);

namespace Container;

/**
 * Defines the PostgreSQL 16 server used by database consumers.
 *
 * @example Inspect the pinned server image without starting Docker
 *     $container = new \Container\PostgreSql16Container();
 *     assert($container->image() === 'postgres:16.6');
 */
final class PostgreSql16Container extends PostgreSqlContainer
{
    protected static $IMAGE = 'postgres:16.6';
}
