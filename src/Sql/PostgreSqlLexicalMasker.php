<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Sql;

use ZtdQuery\Platform\Postgres\Sql\Lexing\QuotedSpan;

/**
 * Masks PostgreSQL comments and string literals without exposing their contents to parsers.
 */
final class PostgreSqlLexicalMasker
{
    /**
     * Replaces string literals with spaces while preserving byte offsets.
     */
    public static function maskStringLiterals(string $sql): string
    {
        $result = '';
        $length = strlen($sql);
        $i = 0;

        while ($i < $length) {
            $char = $sql[$i];
            if ($char === '\'') {
                $tail = substr($sql, $i);
                $quotedLength = QuotedSpan::quotedLength($tail, $char, QuotedSpan::isEscapeStringStart($sql, $i));
                $result .= str_repeat(' ', $quotedLength);
                $i += $quotedLength;
                continue;
            }

            if ($char === '$' && ($i === 0 || !(ctype_alnum($sql[$i - 1]) || $sql[$i - 1] === '_' || $sql[$i - 1] === '$'))) {
                $tail = substr($sql, $i);
                $quotedLength = QuotedSpan::dollarQuotedLength($tail);
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

    /**
     * Replaces each comment with one separator while retaining quoted SQL.
     */
    public static function maskComments(string $sql): string
    {
        $result = '';
        $length = strlen($sql);
        $i = 0;

        while ($i < $length) {
            $char = $sql[$i];

            if ($char === '\'' || $char === '"') {
                $tail = substr($sql, $i);
                $quotedLength = QuotedSpan::quotedLength($tail, $char, QuotedSpan::isEscapeStringStart($sql, $i));
                $result .= substr($tail, 0, $quotedLength);
                $i += $quotedLength;
                continue;
            }

            if ($char === '$' && ($i === 0 || !(ctype_alnum($sql[$i - 1]) || $sql[$i - 1] === '_' || $sql[$i - 1] === '$'))) {
                $tail = substr($sql, $i);
                $quotedLength = QuotedSpan::dollarQuotedLength($tail);
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
                $result .= ' ';
                $i = Lexing\CommentSpan::end($sql, $i);
                continue;
            }

            $result .= $char;
            $i++;
        }

        return $result;
    }
}
