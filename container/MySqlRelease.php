<?php

declare(strict_types=1);

namespace Container;

use RuntimeException;

/**
 * Resolves a MySQL release number to the container that runs it.
 *
 * Tests and fuzz targets start the release named by `MYSQL_VERSION`, so CI runs the
 * newest release and a developer runs any other release locally by setting the variable.
 *
 * @example Resolve the container of a release
 *     assert(\Container\MySqlRelease::container('8.4.7') === \Container\MySql84Container::class);
 *
 * @example Resolve the release named by the environment, falling back to the default
 *     putenv('MYSQL_VERSION');
 *     assert(\Container\MySqlRelease::fromEnvironment() === \Container\MySql84Container::class);
 */
final class MySqlRelease
{
    public const DEFAULT = '8.4.7';

    public const VARIABLE = 'MYSQL_VERSION';

    /**
     * @var array<string, class-string<MySqlContainer>>
     */
    private const CONTAINERS = [
        '5.6.51' => MySql56Container::class,
        '5.7.44' => MySql57Container::class,
        '8.0.44' => MySql80Container::class,
        '8.1.0' => MySql81Container::class,
        '8.2.0' => MySql82Container::class,
        '8.3.0' => MySql83Container::class,
        '8.4.7' => MySql84Container::class,
        '9.0.1' => MySql90Container::class,
        '9.1.0' => MySql91Container::class,
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
     * @param string|null $version Release number such as `8.4.7`, or null for the default
     * @return class-string<MySqlContainer>
     * @throws RuntimeException If no container runs the release
     */
    public static function container(?string $version = null): string
    {
        $version ??= self::DEFAULT;

        return self::CONTAINERS[$version]
            ?? throw new RuntimeException(sprintf('Unknown MySQL version %s; supported versions: %s', $version, implode(', ', self::versions())));
    }

    /**
     * Names the container of the release given by an environment variable.
     *
     * @param string $variable Environment variable holding the release number
     * @return class-string<MySqlContainer>
     * @throws RuntimeException If no container runs the release
     */
    public static function fromEnvironment(string $variable = self::VARIABLE): string
    {
        $version = getenv($variable);

        return self::container($version === false || $version === '' ? null : $version);
    }
}
