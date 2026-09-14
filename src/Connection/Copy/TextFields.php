<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Connection\Copy;

use ValueError;

/**
 * Text fields operations for PostgreSQL copy.
 *
 * @visibility root
 */
final class TextFields
{
    /**
     * Validate separator.
     * @throws ValueError
     */
    public function validateSeparator(string $separator): void
    {
        if (strlen($separator) !== 1) {
            throw new ValueError('PostgreSQL COPY separator must be exactly one byte.');
        }
    }

    /**
     * Escape.
     */
    public function escape(string $value, string $separator): string
    {
        $result = '';
        $length = strlen($value);
        for ($index = 0; $index < $length; $index++) {
            $character = $value[$index];
            $escaped = match ($character) {
                "\x08" => '\\b',
                "\x0C" => '\\f',
                "\n" => '\\n',
                "\r" => '\\r',
                "\t" => '\\t',
                "\x0B" => '\\v',
                '\\' => '\\\\',
                default => null,
            };
            $result .= $escaped ?? ($character === $separator ? '\\' . $character : $character);
        }

        return $result;
    }

    /**
     * @return list<string|null>
     * @throws ValueError
     */
    public function decodeFields(string $row, string $separator, string $nullAs): array
    {
        $values = [];
        $decoded = '';
        $fieldStart = 0;
        $length = strlen($row);
        for ($index = 0; $index <= $length; $index++) {
            if ($index === $length || $row[$index] === $separator) {
                $raw = substr($row, $fieldStart, $index - $fieldStart);
                $values[] = $raw === $nullAs ? null : $decoded;
                $decoded = '';
                $fieldStart = $index + 1;
                continue;
            }

            $character = $row[$index];
            if ($character !== '\\') {
                $decoded .= $character;
                continue;
            }

            $decoded .= $this->decodeEscape($row, $index);
        }

        return $values;
    }
    /**
     * Decodes an escape and advances the caller to its final source byte.
     * @throws ValueError
     */
    public function decodeEscape(string $row, int &$index): string
    {
        $next = $row[$index + 1] ?? null;
        if ($next === null) {
            throw new ValueError('PostgreSQL COPY field ends with an incomplete backslash escape.');
        }
        $index++;
        if ($next >= '0' && $next <= '7') {
            return $this->octalByte($row, $index, $next);
        }
        $following = $row[$index + 1] ?? null;
        if ($next === 'x' && $following !== null && ctype_xdigit($following)) {
            return $this->hexByte($row, $index, $following);
        }
        return match ($next) {
            'b' => "\x08", 'f' => "\x0C", 'n' => "\n", 'r' => "\r", 't' => "\t", 'v' => "\x0B",
            default => $next,
        };
    }

    /**
     * Reads at most three octal digits and rejects values outside one byte.
     * @throws ValueError
     */
    public function octalByte(string $row, int &$index, string $next): string
    {
        $digits = $next;
        while (strlen($digits) < 3) {
            $following = $row[$index + 1] ?? null;
            if ($following === null) {
                break;
            }
            if ($following < '0') {
                break;
            }
            if ($following > '7') {
                break;
            }
            $index++;
            $digit = $following;
            $digits .= $digit;
        }
        $byte = intval($digits, 8);
        if ($byte < 0 || $byte > 255) {
            throw new ValueError('PostgreSQL COPY octal escape must fit in one byte.');
        }
        return chr($byte);
    }

    /**
     * Reads at most two hexadecimal digits after an escape prefix.
     * @throws ValueError
     */
    public function hexByte(string $row, int &$index, string $following): string
    {
        $index++;
        $digit = $following;
        $digits = $digit;
        $following = $row[$index + 1] ?? null;
        if ($following !== null && ctype_xdigit($following)) {
            $index++;
            $digit = $following;
            $digits .= $digit;
        }
        $byte = intval($digits, 16);
        if ($byte < 0) {
            throw new ValueError('PostgreSQL COPY hexadecimal escape must not be negative.');
        }
        if ($byte > 255) {
            throw new ValueError('PostgreSQL COPY hexadecimal escape must fit in one byte.');
        }
        return chr($byte);
    }
}
