<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Connection\Parameter;

use ZtdQuery\Platform\Postgres\Sql\Lexing\CommentSpan;
use ZtdQuery\Platform\Postgres\Sql\Lexing\QuotedSpan;

/**
 * Preserves literal and comment spans before PDO operator detection.
 *
 * @visibility root
 */
final class QuotedInput
{
    /**
     * Copies whitespace or a comment without changing the operand context.
     */
    public function consumeIgnored(EscapeCursor $cursor): bool
    {
        $sql = $cursor->sql;
        $i = $cursor->position;
        if (ctype_space($sql[$i])) {
            $cursor->preserve(1);
            return true;
        }
        $pair = substr($sql, $i, 2);
        if ($pair === '--') {
            $cursor->preserve(strcspn($sql, "\r\n", $i));
            return true;
        }
        if ($pair === '/*') {
            $cursor->preserve(CommentSpan::end($sql, $i) - $i);
            return true;
        }
        return false;
    }

    /**
     * Copies quoted strings and identifiers as single operands.
     */
    public function consumeQuoted(EscapeCursor $cursor): bool
    {
        $sql = $cursor->sql;
        $i = $cursor->position;
        $char = $sql[$i];
        if ($char === "'" || $char === '"') {
            $length = QuotedSpan::quotedLength(substr($sql, $i), $char, $char === "'" && TokenBoundary::isEscapeStringStart($sql, $i));
            $cursor->preserve($length);
            $cursor->expectsOperand = false;
            return true;
        }
        if ($char !== '$') {
            return false;
        }
        $delimiter = TokenBoundary::dollarQuoteDelimiter($sql, $i);
        if ($delimiter === null) {
            return false;
        }
        $end = strpos($sql, $delimiter, $i + strlen($delimiter));
        if ($end === false) {
            $cursor->preserve(strlen($sql) - $i);
            return true;
        }
        $cursor->preserve($end + strlen($delimiter) - $i);
        $cursor->expectsOperand = false;
        return true;
    }
}
