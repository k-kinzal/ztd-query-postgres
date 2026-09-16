<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Connection\Parameter;

use ZtdQuery\Platform\SqlPlaceholderEscaper;

/**
 * Pdo placeholder escaper for PostgreSQL queries.
 */
final class PgSqlPdoPlaceholderEscaper implements SqlPlaceholderEscaper
{
    /**
     * Escapes operator question marks while preserving PDO placeholders and quoted SQL.
     */
    public function escape(string $sql): string
    {
        $cursor = new EscapeCursor($sql);
        $quoted = new QuotedInput();
        $operand = new OperandInput();
        while ($cursor->position < strlen($sql)) {
            if ($quoted->consumeIgnored($cursor) || $quoted->consumeQuoted($cursor)
                || $operand->consumeOperator($cursor) || $operand->consumeOperand($cursor)
            ) {
                continue;
            }
            $char = $sql[$cursor->position];
            $cursor->preserve(1);
            $cursor->expectsOperand = match ($char) {
                ')', ']' => false,
                '(', '[', ',', ';', '.', '=', '<', '>', '!', '~', '+', '-', '*', '/', '%', '^', '|', '&', '#', '@' => true,
                default => $cursor->expectsOperand,
            };
        }
        return $cursor->result;
    }
}
