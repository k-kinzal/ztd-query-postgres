<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Sql\Lexing;

/**
 * Finds nested PostgreSQL block-comment boundaries without inspecting their contents.
 *
 * @visibility root
 */
final class CommentSpan
{
    /**
     * Finds the byte after a nested block comment, including unterminated input.
     */
    public static function end(string $sql, int $start): int
    {
        $length = strlen($sql);
        $scan = $start;
        $depth = 0;
        while (true) {
            $open = strpos($sql, '/*', $scan);
            $close = strpos($sql, '*/', $scan);
            $openPosition = $open === false ? $length : $open;
            $closePosition = $close === false ? $length : $close;
            $markerPosition = min($openPosition, $closePosition);
            if ($markerPosition === $length) {
                return $length;
            }
            $scan = $markerPosition + 2;
            if (substr($sql, $markerPosition, 2) === '/*') {
                $depth++;
                continue;
            }
            $depth--;
            if ($depth === 0) {
                return $scan;
            }
        }
    }
}
