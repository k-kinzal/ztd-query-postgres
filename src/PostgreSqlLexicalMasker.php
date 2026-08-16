<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres;

final class PostgreSqlLexicalMasker
{
    public static function maskStringLiterals(string $sql): string
    {
        $result = '';
        $length = strlen($sql);
        $i = 0;

        while ($i < $length) {
            $char = $sql[$i];
            if ($char === '\'') {
                $tail = substr($sql, $i);
                $quotedLength = self::quotedLength($tail, $char, self::isEscapeStringStart($sql, $i));
                $result .= str_repeat(' ', $quotedLength);
                $i += $quotedLength;
                continue;
            }

            if ($char === '$' && self::isDollarQuoteStart($sql, $i)) {
                $tail = substr($sql, $i);
                $quotedLength = self::dollarQuotedLength($tail);
                if ($quotedLength !== null) {
                    $result .= str_repeat(' ', $quotedLength);
                    $i += $quotedLength;
                    continue;
                }
            }

            $result .= $char;
            $i++;
        }

        return $result;
    }

    public static function maskComments(string $sql): string
    {
        $result = '';
        $length = strlen($sql);
        $i = 0;

        while ($i < $length) {
            $char = $sql[$i];

            if ($char === '\'' || $char === '"') {
                $tail = substr($sql, $i);
                $quotedLength = self::quotedLength($tail, $char, self::isEscapeStringStart($sql, $i));
                $result .= substr($tail, 0, $quotedLength);
                $i += $quotedLength;
                continue;
            }

            if ($char === '$' && self::isDollarQuoteStart($sql, $i)) {
                $tail = substr($sql, $i);
                $quotedLength = self::dollarQuotedLength($tail);
                if ($quotedLength !== null) {
                    $result .= substr($tail, 0, $quotedLength);
                    $i += $quotedLength;
                    continue;
                }
            }

            $pair = substr($sql, $i, 2);
            if ($pair === '--') {
                $commentLength = strcspn($sql, "\r\n", $i);
                $result .= ' ';
                $i += $commentLength;
                continue;
            }

            if ($pair === '/*') {
                $scan = $i;
                $depth = 0;
                while (true) {
                    $open = strpos($sql, '/*', $scan);
                    $close = strpos($sql, '*/', $scan);
                    $openPosition = $open === false ? $length : $open;
                    $closePosition = $close === false ? $length : $close;
                    $markerPosition = min($openPosition, $closePosition);
                    if ($markerPosition === $length) {
                        $scan = $length;
                        break;
                    }
                    $scan = $markerPosition + 2;
                    if (substr($sql, $markerPosition, 2) === '/*') {
                        $depth++;
                        continue;
                    }
                    $depth--;
                    if ($depth === 0) {
                        break;
                    }
                }
                $result .= ' ';
                $i = $scan;
                continue;
            }

            $result .= $char;
            $i++;
        }

        return $result;
    }

    private static function quotedLength(string $sql, string $quote, bool $escapeBackslash): int
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

    private static function dollarQuotedLength(string $sql): ?int
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

    private static function dollarQuoteDelimiter(string $sql): ?string
    {
        $length = strlen($sql);
        $i = 1;
        if ($i < $length && $sql[$i] === '$') {
            return '$$';
        }
        if ($i >= $length || !self::isIdentifierStart($sql[$i])) {
            return null;
        }

        while ($i < $length && (self::isIdentifierStart($sql[$i]) || ctype_digit($sql[$i]))) {
            $i++;
        }
        if ($i >= $length || $sql[$i] !== '$') {
            return null;
        }

        return substr($sql, 0, $i) . '$';
    }

    private static function isDollarQuoteStart(string $sql, int $position): bool
    {
        return $position === 0 || !self::isIdentifierContinuation($sql[$position - 1]);
    }

    private static function isEscapeStringStart(string $sql, int $quotePosition): bool
    {
        $prefix = substr($sql, 0, $quotePosition);

        $escapeMarker = substr($prefix, -1);
        $preceding = substr(substr($prefix, 0, -1), -1);

        return ($escapeMarker === 'E' || $escapeMarker === 'e')
            && ($preceding === '' || !self::isIdentifierContinuation($preceding));
    }

    private static function isIdentifierStart(string $char): bool
    {
        return ctype_alpha($char) || $char === '_';
    }

    private static function isIdentifierContinuation(string $char): bool
    {
        return ctype_alnum($char) || $char === '_' || $char === '$';
    }
}
