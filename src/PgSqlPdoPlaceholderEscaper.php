<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres;

use ZtdQuery\Platform\SqlPlaceholderEscaper;

final class PgSqlPdoPlaceholderEscaper implements SqlPlaceholderEscaper
{
    public function escape(string $sql): string
    {
        $result = '';
        $length = strlen($sql);
        $expectsOperand = true;

        for ($i = 0; $i < $length; $i++) {
            $char = $sql[$i];
            $next = $i + 1 < $length ? $sql[$i + 1] : '';

            if (ctype_space($char)) {
                $result .= $char;
                continue;
            }

            if ($char === '-' && $next === '-') {
                $commentLength = strcspn($sql, "\r\n", $i);
                $result .= substr($sql, $i, $commentLength);
                $i += $commentLength - 1;
                continue;
            }

            if ($char === '/' && $next === '*') {
                $start = $i;
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
                $result .= substr($sql, $start, $scan - $start);
                $i = $scan - 1;
                continue;
            }

            if ($char === '\'' || $char === '"') {
                $quote = $char;
                $escapeBackslash = $quote === '\'' && self::isEscapeStringStart($sql, $i);
                $result .= $char;
                for ($i++; $i < $length; $i++) {
                    $result .= $sql[$i];
                    if ($escapeBackslash && $sql[$i] === '\\') {
                        $escapedCharacter = substr($sql, $i + 1, 1);
                        $result .= $escapedCharacter;
                        $i += strlen($escapedCharacter);
                        continue;
                    }
                    if ($sql[$i] !== $quote) {
                        continue;
                    }
                    if (substr($sql, $i, 2) === $quote . $quote) {
                        $result .= $sql[++$i];
                        continue;
                    }
                    break;
                }
                $expectsOperand = false;
                continue;
            }

            if ($char === '$') {
                $delimiter = self::dollarQuoteDelimiter($sql, $i);
                if ($delimiter !== null) {
                    $end = strpos($sql, $delimiter, $i + strlen($delimiter));
                    if ($end === false) {
                        $result .= substr($sql, $i);
                        break;
                    }
                    $quotedLength = $end + strlen($delimiter) - $i;
                    $result .= substr($sql, $i, $quotedLength);
                    $i += $quotedLength - 1;
                    $expectsOperand = false;
                    continue;
                }
            }

            if ($char === '?') {
                if ($next === '?') {
                    $result .= '??';
                    $i++;
                    $expectsOperand = true;
                    continue;
                }
                if ($next === '|' || $next === '&') {
                    $result .= '??' . $next;
                    $i++;
                    $expectsOperand = true;
                    continue;
                }
                if ($expectsOperand) {
                    $result .= '?';
                    $expectsOperand = false;
                    continue;
                }

                $result .= '??';
                $expectsOperand = true;
                continue;
            }

            if ($char === ':') {
                if ($next === ':') {
                    $result .= '::';
                    $i++;
                    $expectsOperand = true;
                    continue;
                }
            }

            if (self::isIdentifierStart($char)) {
                $colonPrefixed = str_ends_with($result, ':');
                $wordLength = strspn($sql, 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_0123456789$', $i);
                $word = substr($sql, $i, $wordLength);
                $result .= $word;
                $i += $wordLength - 1;
                $expectsOperand = !$colonPrefixed && self::keywordExpectsOperand(strtoupper($word));
                continue;
            }

            if (ctype_digit($char)) {
                $numberLength = strspn($sql, '0123456789.', $i);
                $result .= substr($sql, $i, $numberLength);
                $i += $numberLength - 1;
                $expectsOperand = false;
                continue;
            }

            $result .= $char;
            $expectsOperand = match ($char) {
                ')', ']' => false,
                '(', '[', ',', ';', '.', '=', '<', '>', '!', '~', '+', '-', '*', '/', '%', '^', '|', '&', '#', '@' => true,
                default => $expectsOperand,
            };
        }

        return $result;
    }

    private static function keywordExpectsOperand(string $keyword): bool
    {
        return in_array($keyword, [
            'ALL',
            'AND',
            'ANY',
            'AS',
            'BETWEEN',
            'BY',
            'CASE',
            'CONFLICT',
            'DELETE',
            'DISTINCT',
            'DO',
            'ELSE',
            'FIRST',
            'FROM',
            'HAVING',
            'ILIKE',
            'IN',
            'INSERT',
            'INTO',
            'IS',
            'JOIN',
            'LAST',
            'LIKE',
            'LIMIT',
            'NOT',
            'OFFSET',
            'ON',
            'OR',
            'RETURNING',
            'SELECT',
            'SET',
            'SIMILAR',
            'SOME',
            'THEN',
            'UPDATE',
            'USING',
            'VALUE',
            'VALUES',
            'WHEN',
            'WHERE',
            'ZONE',
        ], true);
    }

    private static function isIdentifierStart(string $char): bool
    {
        return $char >= 'A' && $char <= 'Z'
            || $char >= 'a' && $char <= 'z'
            || $char === '_';
    }

    private static function isIdentifierContinuation(string $char): bool
    {
        return self::isIdentifierStart($char)
            || ctype_digit($char)
            || $char === '$';
    }

    private static function isEscapeStringStart(string $sql, int $quotePosition): bool
    {
        $prefix = substr($sql, 0, $quotePosition);

        return (str_ends_with($prefix, 'E') || str_ends_with($prefix, 'e'))
            && (strlen($prefix) === 1 || !self::isIdentifierContinuation(substr($prefix, -2, 1)));
    }

    private static function dollarQuoteDelimiter(string $sql, int $position): ?string
    {
        $length = strlen($sql);
        $i = $position + 1;
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

        return substr($sql, $position, $i - $position) . '$';
    }
}
