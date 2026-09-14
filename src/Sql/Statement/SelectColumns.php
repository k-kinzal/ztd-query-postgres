<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Sql\Statement;

/**
 * Select columns operations for PostgreSQL statement.
 *
 * @visibility root
 */
final class SelectColumns
{
    /**
     * Extract column names from a SELECT SQL string.
     *
     * @return list<string>
     */
    public function extractSelectColumnNames(string $selectSql): array
    {
        $columns = [];

        if (preg_match('/SELECT\s+(.+?)\s+FROM\b/is', $selectSql, $m) !== 1) {
            if (preg_match('/SELECT\s+(.+)$/is', $selectSql, $m) !== 1) {
                return [];
            }
        }

        $selectList = $m[1];

        if (trim($selectList) === '*') {
            return [];
        }

        $items = $this->splitByTopLevelComma($selectList);
        foreach ($items as $item) {
            $item = trim($item);
            if (preg_match('/\bAS\s+"?([a-zA-Z_]\w*)"?\s*$/i', $item, $aliasMatch) === 1) {
                $columns[] = $aliasMatch[1];
            } elseif (preg_match('/^(?:(?:"[^"]+"|\w+)\.)?("([^"]+)"|([a-zA-Z_]\w*))\s*$/', $item, $colMatch) === 1) {
                $quotedColumn = $colMatch[2] ?? '';
                $columns[] = $quotedColumn !== '' ? $quotedColumn : ($colMatch[3] ?? '');
            } else {
                $replaced = preg_replace('/[^a-zA-Z0-9_]/', '_', $item);
                $columns[] = is_string($replaced) ? $replaced : 'col';
            }
        }

        return $columns;
    }

    /**
     * @return list<string>
     */
    public function splitByTopLevelComma(string $str): array
    {
        $parts = [];
        $current = '';
        $depth = 0;
        $len = strlen($str);

        for ($i = 0; $i < $len; $i++) {
            $char = $str[$i];

            if ($char === "'" || $char === '"') {
                $length = \ZtdQuery\Platform\Postgres\Sql\Lexing\QuotedSpan::quotedLength(substr($str, $i), $char, false);
                $current .= substr($str, $i, $length);
                $i += $length - 1;
                continue;
            }

            if ($char === '(') {
                $depth++;
                $current .= $char;
                continue;
            }

            if ($char === ')') {
                $depth--;
                $current .= $char;
                continue;
            }

            if ($char === ',' && $depth === 0) {
                $parts[] = trim($current);
                $current = '';
                continue;
            }

            $current .= $char;
        }

        $val = trim($current);
        if ($val !== '') {
            $parts[] = $val;
        }

        return $parts;
    }
}
