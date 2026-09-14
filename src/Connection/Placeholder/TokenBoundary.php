<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Connection\Placeholder;

/**
 * Recognizes ASCII token boundaries and operand-expecting keywords for PDO.
 *
 * @visibility root
 */
final class TokenBoundary
{
    /**
     * Keyword expects operand.
     */
    public static function keywordExpectsOperand(string $keyword): bool
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

    /**
     * Is identifier start.
     */
    public static function isIdentifierStart(string $char): bool
    {
        return $char >= 'A' && $char <= 'Z'
            || $char >= 'a' && $char <= 'z'
            || $char === '_';
    }

    /**
     * Is identifier continuation.
     */
    public static function isIdentifierContinuation(string $char): bool
    {
        return self::isIdentifierStart($char)
            || ctype_digit($char)
            || $char === '$';
    }

    /**
     * Is escape string start.
     */
    public static function isEscapeStringStart(string $sql, int $quotePosition): bool
    {
        $prefix = substr($sql, 0, $quotePosition);

        return (str_ends_with($prefix, 'E') || str_ends_with($prefix, 'e'))
            && (strlen($prefix) === 1 || !self::isIdentifierContinuation(substr($prefix, -2, 1)));
    }

    /**
     * Dollar quote delimiter.
     */
    public static function dollarQuoteDelimiter(string $sql, int $position): ?string
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
