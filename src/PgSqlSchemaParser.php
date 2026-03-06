<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres;

use ZtdQuery\Schema\ColumnType;
use ZtdQuery\Schema\ColumnTypeFamily;
use ZtdQuery\Platform\SchemaParser;
use ZtdQuery\Schema\TableDefinition;

/**
 * PostgreSQL implementation of SchemaParser.
 *
 * Parses CREATE TABLE statements using regex-based approach
 * to extract column definitions, types, constraints, and keys.
 */
final class PgSqlSchemaParser implements SchemaParser
{
    /**
     * {@inheritDoc}
     */
    public function parse(string $createTableSql): ?TableDefinition
    {
        if (preg_match('/^\s*CREATE\s+(?:TEMPORARY\s+|TEMP\s+|UNLOGGED\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:"[^"]+"|[a-zA-Z_]\w*(?:\."[^"]+"|\.(?:[a-zA-Z_]\w*))?)\s*\((.+)\)/is', $createTableSql, $m) !== 1) {
            return null;
        }

        $body = $m[1];

        $columns = [];
        $columnTypes = [];
        /** @var array<string, ColumnType> $typedColumns */
        $typedColumns = [];
        $primaryKeys = [];
        $notNullColumns = [];
        /** @var array<string, list<string>> $uniqueConstraints */
        $uniqueConstraints = [];
        $uniqueIndex = 0;

        $entries = $this->splitTableBody($body);

        foreach ($entries as $entry) {
            $entry = trim($entry);
            if ($entry === '') {
                continue;
            }

            if ($this->isConstraintEntry($entry)) {
                $this->parseConstraint($entry, $primaryKeys, $uniqueConstraints, $uniqueIndex);
                continue;
            }

            $columnDef = $this->parseColumnDefinition($entry);
            if ($columnDef === null) {
                continue;
            }

            $columns[] = $columnDef['name'];
            $columnTypes[$columnDef['name']] = $columnDef['type'];
            $typedColumns[$columnDef['name']] = $columnDef['columnType'];

            if ($columnDef['notNull']) {
                $notNullColumns[] = $columnDef['name'];
            }

            if ($columnDef['primaryKey']) {
                $primaryKeys[] = $columnDef['name'];
                if (!in_array($columnDef['name'], $notNullColumns, true)) {
                    $notNullColumns[] = $columnDef['name'];
                }
            }

            if ($columnDef['unique']) {
                $keyName = $columnDef['name'] . '_UNIQUE';
                $uniqueConstraints[$keyName] = [$columnDef['name']];
            }
        }

        if ($columns === []) {
            return null;
        }

        foreach ($uniqueConstraints as $constraintColumns) {
            foreach ($constraintColumns as $col) {
                if (!in_array($col, $columns, true)) {
                    return null;
                }
            }
        }

        return new TableDefinition(
            $columns,
            $columnTypes,
            $primaryKeys,
            $notNullColumns,
            $uniqueConstraints,
            $typedColumns,
        );
    }

    /**
     * @return array{name: string, type: string, columnType: ColumnType, notNull: bool, primaryKey: bool, unique: bool}|null
     */
    private function parseColumnDefinition(string $entry): ?array
    {
        if (preg_match('/^("[^"]+"|[a-zA-Z_]\w*)\s+(.+)$/is', $entry, $m) !== 1) {
            return null;
        }

        $name = $this->unquoteIdentifier($m[1]);
        $rest = trim($m[2]);

        $typeInfo = $this->extractType($rest);
        if ($typeInfo === null) {
            return null;
        }

        $nativeType = $typeInfo['type'];
        $afterType = $typeInfo['rest'];

        $notNull = preg_match('/\bNOT\s+NULL\b/i', $afterType) === 1;
        $primaryKey = preg_match('/\bPRIMARY\s+KEY\b/i', $afterType) === 1;
        $unique = preg_match('/\bUNIQUE\b/i', $afterType) === 1;

        $family = $this->mapTypeToFamily($nativeType);
        $columnType = new ColumnType($family, strtoupper($nativeType));

        return [
            'name' => $name,
            'type' => strtoupper($nativeType),
            'columnType' => $columnType,
            'notNull' => $notNull,
            'primaryKey' => $primaryKey,
            'unique' => $unique,
        ];
    }

    /**
     * Known multi-word PostgreSQL type prefixes (first word).
     * Used to determine whether a second word is part of the type name
     * or a column constraint keyword.
     */
    private const MULTI_WORD_TYPE_PREFIXES = [
        'DOUBLE', 'CHARACTER', 'TIME', 'TIMESTAMP',
        'BIT', 'INTERVAL',
    ];

    /**
     * Column constraint keywords that should NOT be consumed as part of the type name.
     */
    private const CONSTRAINT_KEYWORDS = [
        'PRIMARY', 'NOT', 'NULL', 'UNIQUE', 'CHECK', 'DEFAULT',
        'REFERENCES', 'COLLATE', 'CONSTRAINT', 'GENERATED', 'DEFERRABLE',
    ];

    /**
     * @return array{type: string, rest: string}|null
     */
    private function extractType(string $str): ?array
    {
        $str = ltrim($str);

        if (preg_match('/^([a-zA-Z_]\w*)/i', $str, $m) !== 1) {
            return null;
        }

        $baseType = $m[1];
        $pos = strlen($baseType);
        $rest = substr($str, $pos);

        if (in_array(strtoupper($baseType), self::MULTI_WORD_TYPE_PREFIXES, true)) {
            $trimmedRest = ltrim($rest);
            if (preg_match('/^([a-zA-Z_]\w*)/i', $trimmedRest, $m2) === 1) {
                $secondWord = $m2[1];
                if (!in_array(strtoupper($secondWord), self::CONSTRAINT_KEYWORDS, true)) {
                    $baseType .= ' ' . $secondWord;
                    $pos = strlen($str) - strlen($trimmedRest) + strlen($secondWord);
                    $rest = substr($str, $pos);
                }
            }
        }

        $params = '';
        $trimmedRest = ltrim($rest);
        if (str_starts_with($trimmedRest, '(')) {
            if (preg_match('/^(\([^)]*\))/', $trimmedRest, $pm) === 1) {
                $params = $pm[1];
                $rest = substr($trimmedRest, strlen($params));
            }
        } else {
            $rest = $trimmedRest;
        }

        $arrayBrackets = '';
        $trimmedRest = ltrim($rest);
        while (preg_match('/^\[\s*\]/', $trimmedRest, $ab) === 1) {
            $arrayBrackets .= $ab[0];
            $trimmedRest = ltrim(substr($trimmedRest, strlen($ab[0])));
        }

        $fullType = $baseType . $params . $arrayBrackets;

        return ['type' => $fullType, 'rest' => trim($trimmedRest)];
    }

    private function mapTypeToFamily(string $nativeType): ColumnTypeFamily
    {
        $upper = strtoupper(preg_replace('/\(.*\)/', '', $nativeType) ?? $nativeType);
        $upper = trim($upper);

        $upper = preg_replace('/\[\s*\]/', '', $upper) ?? $upper;
        $upper = trim($upper);

        return match ($upper) {
            'INT', 'INT2', 'INT4', 'INT8',
            'INTEGER', 'SMALLINT', 'BIGINT',
            'SERIAL', 'SMALLSERIAL', 'BIGSERIAL' => ColumnTypeFamily::INTEGER,
            'REAL', 'FLOAT4' => ColumnTypeFamily::FLOAT,
            'DOUBLE PRECISION', 'FLOAT8' => ColumnTypeFamily::DOUBLE,
            'DECIMAL', 'NUMERIC' => ColumnTypeFamily::DECIMAL,
            'CHAR', 'CHARACTER', 'VARCHAR', 'CHARACTER VARYING',
            'TEXT', 'CITEXT', 'NAME' => $this->mapStringType($upper),
            'BOOLEAN', 'BOOL' => ColumnTypeFamily::BOOLEAN,
            'DATE' => ColumnTypeFamily::DATE,
            'TIME', 'TIMETZ', 'TIME WITH TIME ZONE',
            'TIME WITHOUT TIME ZONE' => ColumnTypeFamily::TIME,
            'TIMESTAMP', 'TIMESTAMPTZ',
            'TIMESTAMP WITH TIME ZONE',
            'TIMESTAMP WITHOUT TIME ZONE' => ColumnTypeFamily::TIMESTAMP,
            'BYTEA' => ColumnTypeFamily::BINARY,
            'JSON', 'JSONB' => ColumnTypeFamily::JSON,
            default => ColumnTypeFamily::UNKNOWN,
        };
    }

    private function mapStringType(string $upper): ColumnTypeFamily
    {
        if ($upper === 'TEXT' || $upper === 'CITEXT') {
            return ColumnTypeFamily::TEXT;
        }

        return ColumnTypeFamily::STRING;
    }

    /**
     * @param list<string> $primaryKeys
     * @param array<string, list<string>> $uniqueConstraints
     */
    private function parseConstraint(string $entry, array &$primaryKeys, array &$uniqueConstraints, int &$uniqueIndex): void
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

    private function isConstraintEntry(string $entry): bool
    {
        $upper = strtoupper(ltrim($entry));

        return str_starts_with($upper, 'CONSTRAINT ')
            || str_starts_with($upper, 'PRIMARY KEY')
            || str_starts_with($upper, 'UNIQUE')
            || str_starts_with($upper, 'CHECK')
            || str_starts_with($upper, 'FOREIGN KEY')
            || str_starts_with($upper, 'EXCLUDE');
    }

    /**
     * @return list<string>
     */
    private function parseColumnRefList(string $str): array
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
     * Split table body by top-level commas (respecting parentheses).
     *
     * @return list<string>
     */
    private function splitTableBody(string $body): array
    {
        $entries = [];
        $current = '';
        $depth = 0;
        $inSingleQuote = false;
        $inDoubleQuote = false;
        $len = strlen($body);

        for ($i = 0; $i < $len; $i++) {
            $char = $body[$i];

            if ($inSingleQuote) {
                $current .= $char;
                if ($char === "'" && isset($body[$i + 1]) && $body[$i + 1] === "'") {
                    $current .= "'";
                    $i++;
                } elseif ($char === "'") {
                    $inSingleQuote = false;
                }
                continue;
            }

            if ($inDoubleQuote) {
                $current .= $char;
                if ($char === '"' && isset($body[$i + 1]) && $body[$i + 1] === '"') {
                    $current .= '"';
                    $i++;
                } elseif ($char === '"') {
                    $inDoubleQuote = false;
                }
                continue;
            }

            if ($char === "'") {
                $current .= $char;
                $inSingleQuote = true;
                continue;
            }

            if ($char === '"') {
                $current .= $char;
                $inDoubleQuote = true;
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
                $entries[] = trim($current);
                $current = '';
                continue;
            }

            $current .= $char;
        }

        $val = trim($current);
        if ($val !== '') {
            $entries[] = $val;
        }

        return $entries;
    }

    private function unquoteIdentifier(string $identifier): string
    {
        $trimmed = trim($identifier);
        if (str_starts_with($trimmed, '"') && str_ends_with($trimmed, '"')) {
            return str_replace('""', '"', substr($trimmed, 1, -1));
        }

        return $trimmed;
    }
}
