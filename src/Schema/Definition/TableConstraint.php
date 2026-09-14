<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Schema\Definition;

/**
 * Table constraint operations for PostgreSQL definition.
 *
 * @visibility root
 */
final class TableConstraint
{
    /**
     * @param list<string> $primaryKeys
     * @param array<string, list<string>> $uniqueConstraints
     */
    public function parseConstraint(string $entry, array &$primaryKeys, array &$uniqueConstraints, int &$uniqueIndex): void
    {
        if (preg_match('/PRIMARY\s+KEY\s*\(([^)]+)\)/i', $entry, $m) === 1) {
            $cols = $this->parseColumnRefList($m[1]);
            foreach ($cols as $col) {
                if (!in_array($col, $primaryKeys, true)) {
                    $primaryKeys[] = $col;
                }
            }

            return;
        }

        if (preg_match('/UNIQUE\s*\(([^)]+)\)/i', $entry, $m) === 1) {
            $cols = $this->parseColumnRefList($m[1]);
            if ($cols !== []) {
                $keyName = 'unique_' . $uniqueIndex++;
                if (preg_match('/CONSTRAINT\s+("[^"]+"|[a-zA-Z_]\w*)/i', $entry, $cNameMatch) === 1) {
                    $keyName = $this->unquoteIdentifier($cNameMatch[1]);
                }
                $uniqueConstraints[$keyName] = $cols;
            }
        }
    }

    /**
     * Is constraint entry.
     */
    public function isConstraintEntry(string $entry): bool
    {
        $length = strspn($entry, 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_$');
        $leadingKeyword = strtoupper(substr($entry, 0, $length));

        return in_array($leadingKeyword, [
            'CONSTRAINT',
            'PRIMARY',
            'UNIQUE',
            'CHECK',
            'FOREIGN',
            'EXCLUDE',
        ], true);
    }

    /**
     * @return list<string>
     */
    public function parseColumnRefList(string $str): array
    {
        $cols = [];
        foreach (explode(',', $str) as $part) {
            $col = trim($part);
            $col = $this->unquoteIdentifier($col);
            if ($col !== '') {
                $cols[] = $col;
            }
        }

        return $cols;
    }

    /**
     * Unquote identifier.
     */
    public function unquoteIdentifier(string $identifier): string
    {
        $trimmed = trim($identifier);
        if (str_starts_with($trimmed, '"') && str_ends_with($trimmed, '"')) {
            return str_replace('""', '"', substr($trimmed, 1, -1));
        }

        return $trimmed;
    }
}
