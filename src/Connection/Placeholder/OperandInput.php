<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Connection\Placeholder;

/**
 * Tracks PostgreSQL operands and operators when escaping question marks for PDO.
 *
 * @visibility root
 */
final class OperandInput
{
    /**
     * Distinguishes placeholders from question-mark operators and type casts.
     */
    public function consumeOperator(EscapeCursor $cursor): bool
    {
        $char = $cursor->sql[$cursor->position];
        $next = substr($cursor->sql, $cursor->position + 1, 1);
        if ($char === ':' && $next === ':') {
            $cursor->preserve(2);
            $cursor->expectsOperand = true;
            return true;
        }
        if ($char !== '?') {
            return false;
        }
        if ($next === '?') {
            $cursor->preserve(2);
            $cursor->expectsOperand = true;
        } elseif ($next === '|' || $next === '&') {
            $cursor->result .= '??' . $next;
            $cursor->position += 2;
            $cursor->expectsOperand = true;
        } else {
            $cursor->result .= $cursor->expectsOperand ? '?' : '??';
            $cursor->position++;
            $cursor->expectsOperand = !$cursor->expectsOperand;
        }
        return true;
    }

    /**
     * Copies words and numeric operands while tracking keyword context.
     */
    public function consumeOperand(EscapeCursor $cursor): bool
    {
        $sql = $cursor->sql;
        $i = $cursor->position;
        if (TokenBoundary::isIdentifierStart($sql[$i])) {
            $colonPrefixed = str_ends_with($cursor->result, ':');
            $length = strspn($sql, 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_0123456789$', $i);
            $word = substr($sql, $i, $length);
            $cursor->preserve($length);
            $cursor->expectsOperand = !$colonPrefixed && TokenBoundary::keywordExpectsOperand(strtoupper($word));
            return true;
        }
        if (ctype_digit($sql[$i])) {
            $cursor->preserve(strspn($sql, '0123456789.', $i));
            $cursor->expectsOperand = false;
            return true;
        }
        return false;
    }
}
