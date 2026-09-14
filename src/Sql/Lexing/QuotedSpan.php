<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Sql\Lexing;

/**
 * Recognizes the boundaries of PostgreSQL quoted lexical spans.
 *
 * @visibility root
 */
final class QuotedSpan
{
    /**
     * Measures a quoted span, including doubled quotes and optional backslash escapes.
     */
    public static function quotedLength(string $sql, string $quote, bool $escapeBackslash): int
    {
        $length = strlen($sql);
        $i = 1;

        while ($i < $length) {
            if ($escapeBackslash && $sql[$i] === '\\') {
                $i += strlen(substr($sql, $i, 2));
                continue;
            }
            if ($sql[$i] !== $quote) {
                $i++;
                continue;
            }
            if (str_starts_with(substr($sql, $i), $quote . $quote)) {
                $i += 2;
                continue;
            }
            $i++;
            break;
        }

        return $i;
    }

    /**
     * Measures a dollar-quoted span, or returns null when no delimiter begins here.
     */
    public static function dollarQuotedLength(string $sql): ?int
    {
        $delimiter = self::dollarQuoteDelimiter($sql);
        if ($delimiter === null) {
            return null;
        }

        $end = strpos($sql, $delimiter, strlen($delimiter));
        if ($end === false) {
            return strlen($sql);
        }

        return $end + strlen($delimiter);
    }

    /**
     * Reads a PostgreSQL dollar-quote delimiter at the beginning of a string.
     */
    public static function dollarQuoteDelimiter(string $sql): ?string
    {
        $length = strlen($sql);
        $i = 1;
        if ($i < $length && $sql[$i] === '$') {
            return '$$';
        }
        if ($i >= $length || !(ctype_alpha($sql[$i]) || $sql[$i] === '_')) {
            return null;
        }

        while ($i < $length && ((ctype_alpha($sql[$i]) || $sql[$i] === '_') || ctype_digit($sql[$i]))) {
            $i++;
        }
        if ($i >= $length || $sql[$i] !== '$') {
            return null;
        }

        return substr($sql, 0, $i) . '$';
    }

    /**
     * Recognizes an E prefix separated from an identifier before a quote.
     */
    public static function isEscapeStringStart(string $sql, int $quotePosition): bool
    {
        $prefix = substr($sql, 0, $quotePosition);

        $escapeMarker = substr($prefix, -1);
        $preceding = substr(substr($prefix, 0, -1), -1);

        return ($escapeMarker === 'E' || $escapeMarker === 'e')
            && ($preceding === '' || !(ctype_alnum($preceding) || $preceding === '_' || $preceding === '$'));
    }
}
