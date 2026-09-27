<?php

declare(strict_types=1);

namespace Container;

use RuntimeException;

/**
 * Resolves a PostgreSQL release number to the container that runs it.
 *
 * Tests and fuzz targets start the release named by `PG_VERSION`, so CI runs the
 * newest release and a developer runs any other release locally by setting the variable.
 *
 * @example Resolve the container of a release
 *     assert(\Container\PostgreSqlRelease::container('16.6') === \Container\PostgreSql16Container::class);
 *
 * @example Resolve the release named by the environment, falling back to the default
 *     putenv('PG_VERSION');
 *     assert(\Container\PostgreSqlRelease::fromEnvironment() === \Container\PostgreSql17Container::class);
 */
final class PostgreSqlRelease
{
    public const DEFAULT = '17.2';

    public const VARIABLE = 'PG_VERSION';

    /**
     * @var array<string, class-string<PostgreSqlContainer>>
     */
    private const CONTAINERS = [
        '16.6' => PostgreSql16Container::class,
        '17.2' => PostgreSql17Container::class,
    ];

    /**
     * Lists the release numbers with a container, oldest first.
     *
     * @return list<string>
     */
    public static function versions(): array
    {
        return array_keys(self::CONTAINERS);
    }

    /**
     * Names the newest release with a container.
     */
    public static function latest(): string
    {
        return self::versions()[count(self::CONTAINERS) - 1];
    }

    /**
     * Names the container of a release.
     *
     * @param string|null $version Release number such as `17.2`, or null for the default
     * @return class-string<PostgreSqlContainer>
     * @throws RuntimeException If no container runs the release
     */
    public static function container(?string $version = null): string
    {
        $version ??= self::DEFAULT;

        return self::CONTAINERS[$version]
            ?? throw new RuntimeException(sprintf('Unknown PostgreSQL version %s; supported versions: %s', $version, implode(', ', self::versions())));
    }

    /**
     * Names the container of the release given by an environment variable.
     *
     * @param string $variable Environment variable holding the release number
     * @return class-string<PostgreSqlContainer>
     * @throws RuntimeException If no container runs the release
     */
    public static function fromEnvironment(string $variable = self::VARIABLE): string
    {
        $version = getenv($variable);

        return self::container($version === false || $version === '' ? null : $version);
    }
}
