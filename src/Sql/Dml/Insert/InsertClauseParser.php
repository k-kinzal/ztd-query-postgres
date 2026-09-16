<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Sql\Dml\Insert;

use ZtdQuery\Platform\Postgres\Sql\PostgreSqlLexicalMasker;

/**
 * Extracts PostgreSQL INSERT columns, values and query sources.
 *
 * @visibility root
 */
final class InsertClauseParser
{
    /**
     * Extract column list from INSERT statement.
     *
     * @return list<string>
     */
    public function extractInsertColumns(string $sql): array
    {
        $sql = PostgreSqlLexicalMasker::maskComments($sql);
        if (preg_match('/INSERT\s+INTO\s+(?:ONLY\s+)?(?:"[^"]+"|[a-zA-Z_]\w*(?:\."[^"]+"|\.(?:[a-zA-Z_]\w*))?)\s*\(([^)]+)\)\s*(?:VALUES|SELECT|DEFAULT)/i', $sql, $m) === 1) {
            return (new \ZtdQuery\Platform\Postgres\Sql\Lexing\IdentifierDecoder())->parseColumnList($m[1]);
        }

        return [];
    }

    /**
     * @return array{keyword: string, offset: int}|null
     */
    public function findInsertSourceClause(string $sql): ?array
    {
        $searchable = PostgreSqlLexicalMasker::maskStringLiterals($sql);
        $length = strlen($searchable);
        $depth = 0;
        $foundInsert = false;

        for ($i = 0; $i < $length; $i++) {
            if ($searchable[$i] === '"') {
                $end = strpos($searchable, '"', $i + 1);
                $i = $end === false ? $length : $end;
                continue;
            }

            if ($searchable[$i] === '(') {
                $depth++;
                continue;
            }

            if ($searchable[$i] === ')') {
                if ($depth > 0) {
                    $depth--;
                }
                continue;
            }

            $tokenLength = strspn(
                $searchable,
                'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_$',
                $i,
            );
            if ($tokenLength === 0) {
                continue;
            }

            if ($depth === 0 && ctype_alpha($searchable[$i])) {
                $keyword = strtoupper(substr($searchable, $i, $tokenLength));
                if (!$foundInsert) {
                    $foundInsert = $keyword === 'INSERT';
                } elseif ($keyword === 'VALUES' || $keyword === 'SELECT') {
                    return ['keyword' => $keyword, 'offset' => $i];
                }
            }

            $i += $tokenLength - 1;
        }

        return null;
    }

    /**
     * @return array{items: list<string>, end: int}|null
     */
    public function extractParenthesizedList(string $str, int $start): ?array
    {
        if (!isset($str[$start]) || $str[$start] !== '(') {
            return null;
        }

        $items = [];
        $current = '';
        $depth = 0;
        $bracketDepth = 0;
        $len = strlen($str);

        for ($pos = $start; $pos < $len; $pos++) {
            $char = $str[$pos];

            if ($char === "'") {
                $length = \ZtdQuery\Platform\Postgres\Sql\Lexing\QuotedSpan::quotedLength(substr($str, $pos), $char, false);
                $current .= substr($str, $pos, $length);
                $pos += $length - 1;
                continue;
            }

            if ($char === '(') {
                $depth++;
                if ($depth === 1) {
                    continue;
                }
                $current .= $char;
                continue;
            }

            if ($char === ')') {
                $depth--;
                if ($depth === 0) {
                    $val = trim($current);
                    if ($val !== '') {
                        $items[] = $val;
                    }

                    return ['items' => $items, 'end' => $pos + 1];
                }
                $current .= $char;
                continue;
            }

            $this->appendListCharacter($char, $depth, $bracketDepth, $items, $current);
        }

        return null;
    }

    /**
     * Extract VALUES rows from INSERT statement.
     *
     * @return list<list<string>>
     */
    public function extractInsertValues(string $sql): array
    {
        $sql = PostgreSqlLexicalMasker::maskComments($sql);
        $rows = [];
        $source = $this->findInsertSourceClause($sql);
        if ($source === null) {
            return [];
        }
        if ($source['keyword'] !== 'VALUES') {
            return [];
        }

        $rest = substr($sql, $source['offset'] + strlen($source['keyword']));
        $pos = 0;
        $len = strlen($rest);

        while ($pos < $len) {
            while ($pos < $len && ($rest[$pos] === ' ' || $rest[$pos] === "\n" || $rest[$pos] === "\r" || $rest[$pos] === "\t" || $rest[$pos] === ',')) {
                $pos++;
            }

            if ($pos >= $len || $rest[$pos] !== '(') {
                break;
            }

            $values = $this->extractParenthesizedList($rest, $pos);
            if ($values === null) {
                break;
            }
            $rows[] = $values['items'];
            $pos = $values['end'];
        }

        return $rows;
    }

    /**
     * Check if INSERT has a SELECT subquery (INSERT ... SELECT).
     */
    public function hasInsertSelect(string $sql): bool
    {
        $sql = PostgreSqlLexicalMasker::maskComments($sql);
        $source = $this->findInsertSourceClause($sql);

        return $source !== null && $source['keyword'] === 'SELECT';
    }

    /**
     * Extract the SELECT part from INSERT ... SELECT.
     */
    public function extractInsertSelectSql(string $sql): ?string
    {
        $sql = PostgreSqlLexicalMasker::maskComments($sql);
        $source = $this->findInsertSourceClause($sql);
        if ($source !== null && $source['keyword'] === 'SELECT') {
            return substr($sql, $source['offset']);
        }

        return null;
    }

    /**
     * Appends a value character, treating array brackets as comma nesting.
     * @param list<string> $items
     */
    public function appendListCharacter(string $char, int $depth, int &$bracketDepth, array &$items, string &$current): void
    {
        if ($char === '[') {
            $bracketDepth++;
            $current .= $char;
            return;
        }

        if ($char === ']' && $bracketDepth > 0) {
            $bracketDepth--;
            $current .= $char;
            return;
        }

        if ($char === ',' && $depth === 1 && $bracketDepth === 0) {
            $items[] = trim($current);
            $current = '';
            return;
        }

        $current .= $char;
    }
}
