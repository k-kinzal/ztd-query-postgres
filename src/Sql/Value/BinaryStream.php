<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Sql\Value;

/**
 * Reads complete byte streams for PostgreSQL binary literals.
 *
 * @visibility root
 */
final class BinaryStream
{
    /**
     * Reads a binary value without moving the caller's stream position.
     * @param resource $stream
     */
    public function read($stream): string
    {
        $position = ftell($stream);
        rewind($stream);
        $contents = stream_get_contents($stream);
        if ($position !== false) {
            fseek($stream, $position);
        }

        return $contents === false ? '' : $contents;
    }
}
