<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Sql\Statement;

use ZtdQuery\Platform\Postgres\Sql\PostgreSqlLexicalMasker;

/**
 * Classification operations for PostgreSQL statement.
 *
 * @visibility root
 */
final class StatementClassifier
{
    /**
     * @return 'SELECT'|'INSERT'|'UPDATE'|'DELETE'|'MERGE'|null
     */
    public function classifyWithStatement(string $sql): ?string
    {
        $stripped = PostgreSqlLexicalMasker::maskStringLiterals($sql);
        $len = strlen($stripped);
        $depth = 0;
        $seenCteBody = false;

        for ($i = 0; $i < $len; $i++) {
            $char = $stripped[$i];

            if ($char === '"') {
                $end = strpos($stripped, '"', $i + 1);
                $i = $end === false ? $len : $end;
                continue;
            }

            if ($char === '(') {
                $depth++;
                $seenCteBody = true;
                continue;
            }

            if ($char === ')') {
                if ($depth > 0) {
                    $depth--;
                }
                continue;
            }

            if (!$seenCteBody || $depth !== 0 || !ctype_alpha($char)) {
                continue;
            }

            $word = $this->keywordAt($stripped, $i);
            if ($word === null) {
                continue;
            }
            $result = $word['keyword'];
            if ($result !== null) {
                return $result;
            }

            $i = $word['end'] - 1;
        }

        return null;
    }

    /**
     * @return 'SELECT'|'INSERT'|'UPDATE'|'DELETE'|'MERGE'|'TRUNCATE'|'CREATE_TABLE'|'DROP_TABLE'|'ALTER_TABLE'|'DO'|'TCL'|null
     */
    public function classifySimpleStatement(string $sql): ?string
    {
        $trimmed = ltrim($sql);

        if (preg_match('/^SELECT\b/i', $trimmed) === 1) {
            return 'SELECT';
        }
        if (preg_match('/^INSERT\b/i', $trimmed) === 1) {
            return 'INSERT';
        }
        if (preg_match('/^UPDATE\b/i', $trimmed) === 1) {
            return 'UPDATE';
        }
        if (preg_match('/^DELETE\b/i', $trimmed) === 1) {
            return 'DELETE';
        }
        if (preg_match('/^MERGE\b/i', $trimmed) === 1) {
            return 'MERGE';
        }
        if (preg_match('/^TRUNCATE\b/i', $trimmed) === 1) {
            return 'TRUNCATE';
        }
        if (preg_match('/^CREATE\s+(?:TEMPORARY\s+|TEMP\s+|UNLOGGED\s+)?TABLE\b/i', $trimmed) === 1) {
            return 'CREATE_TABLE';
        }
        if (preg_match('/^DROP\s+TABLE\b/i', $trimmed) === 1) {
            return 'DROP_TABLE';
        }
        if (preg_match('/^ALTER\s+TABLE\b/i', $trimmed) === 1) {
            return 'ALTER_TABLE';
        }
        if (preg_match('/^DO(?:\s|$)/i', $trimmed) === 1) {
            return 'DO';
        }

        if (preg_match('/^(?:BEGIN|START\s+TRANSACTION|COMMIT|ROLLBACK|SAVEPOINT|RELEASE\s+SAVEPOINT|SET\s+TRANSACTION)\b/i', $trimmed) === 1) {
            return 'TCL';
        }

        return null;
    }
    /**
     * Reads a statement keyword only at an unquoted word boundary.
     * @return array{keyword: 'SELECT'|'INSERT'|'UPDATE'|'DELETE'|'MERGE'|null, end: int}|null
     */
    public function keywordAt(string $stripped, int $position): ?array
    {
        $len = strlen($stripped);
        $prev = $position > 0 ? $stripped[$position - 1] : ' ';
        if (ctype_alpha($prev) || $prev === '_') {
            return null;
        }

        $j = $position;
        while ($j < $len && (ctype_alpha($stripped[$j]) || $stripped[$j] === '_')) {
            $j++;
        }

        $keyword = strtoupper(substr($stripped, $position, $j - $position));

        $result = match ($keyword) {
            'SELECT' => 'SELECT',
            'INSERT' => 'INSERT',
            'UPDATE' => 'UPDATE',
            'DELETE' => 'DELETE',
            'MERGE' => 'MERGE',
            default => null,
        };

        return ['keyword' => $result, 'end' => $j];
    }

    /**
     * Classify a SQL statement type.
     *
     * @return 'SELECT'|'INSERT'|'UPDATE'|'DELETE'|'MERGE'|'TRUNCATE'|'CREATE_TABLE'|'DROP_TABLE'|'ALTER_TABLE'|'DO'|'TCL'|null
     *
     */
    public function classifyStatement(string $sql): ?string
    {
        $trimmed = PostgreSqlLexicalMasker::maskComments($sql);

        if (preg_match('/^\s*WITH\b/i', $trimmed) === 1) {
            return $this->classifyWithStatement($trimmed);
        }

        return $this->classifySimpleStatement($trimmed);
    }
}
