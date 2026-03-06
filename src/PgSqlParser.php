<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres;

/**
 * Focused PostgreSQL SQL parser.
 *
 * Handles the ZTD-required SQL subset: SELECT, INSERT, UPDATE, DELETE,
 * CREATE TABLE, DROP TABLE, ALTER TABLE, TRUNCATE.
 *
 * Uses regex + recursive descent hybrid approach to extract structural
 * information without a full PostgreSQL grammar parser.
 */
final class PgSqlParser
{
    /**
     * Classify a SQL statement type.
     *
     * @return string|null One of: 'SELECT', 'INSERT', 'UPDATE', 'DELETE',
     *                     'TRUNCATE', 'CREATE_TABLE', 'DROP_TABLE', 'ALTER_TABLE', or null.
     */
    public function classifyStatement(string $sql): ?string
    {
        $trimmed = $this->stripLeadingComments($sql);

        if (preg_match('/^\s*WITH\b/i', $trimmed) === 1) {
            return $this->classifyWithStatement($trimmed);
        }

        return $this->classifySimpleStatement($trimmed);
    }

    /**
     * Split SQL string into individual statements.
     *
     * @return list<string>
     */
    public function splitStatements(string $sql): array
    {
        $statements = [];
        $current = '';
        $len = strlen($sql);
        $inSingleQuote = false;
        $inDoubleQuote = false;
        $inDollarQuote = false;
        $dollarTag = '';
        $inLineComment = false;
        $inBlockComment = false;

        for ($i = 0; $i < $len; $i++) {
            $char = $sql[$i];
            $next = $i + 1 < $len ? $sql[$i + 1] : '';

            if ($inLineComment) {
                $current .= $char;
                if ($char === "\n") {
                    $inLineComment = false;
                }
                continue;
            }

            if ($inBlockComment) {
                $current .= $char;
                if ($char === '*' && $next === '/') {
                    $current .= '/';
                    $i++;
                    $inBlockComment = false;
                }
                continue;
            }

            if ($inDollarQuote) {
                if ($char === '$') {
                    $endTag = '$' . $dollarTag . '$';
                    $remaining = substr($sql, $i, strlen($endTag));
                    if ($remaining === $endTag) {
                        $current .= $endTag;
                        $i += strlen($endTag) - 1;
                        $inDollarQuote = false;
                        continue;
                    }
                }
                $current .= $char;
                continue;
            }

            if ($inSingleQuote) {
                $current .= $char;
                if ($char === "'" && $next === "'") {
                    $current .= "'";
                    $i++;
                } elseif ($char === "'") {
                    $inSingleQuote = false;
                }
                continue;
            }

            if ($inDoubleQuote) {
                $current .= $char;
                if ($char === '"' && $next === '"') {
                    $current .= '"';
                    $i++;
                } elseif ($char === '"') {
                    $inDoubleQuote = false;
                }
                continue;
            }

            if ($char === '-' && $next === '-') {
                $current .= $char;
                $inLineComment = true;
                continue;
            }

            if ($char === '/' && $next === '*') {
                $current .= $char;
                $inBlockComment = true;
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

            if ($char === '$') {
                $tag = $this->extractDollarTag($sql, $i);
                if ($tag !== null) {
                    $dollarTag = $tag;
                    $fullTag = '$' . $tag . '$';
                    $current .= $fullTag;
                    $i += strlen($fullTag) - 1;
                    $inDollarQuote = true;
                    continue;
                }
            }

            if ($char === ';') {
                $stmt = trim($current);
                if ($stmt !== '') {
                    $statements[] = $stmt;
                }
                $current = '';
                continue;
            }

            $current .= $char;
        }

        $stmt = trim($current);
        if ($stmt !== '') {
            $statements[] = $stmt;
        }

        return $statements;
    }

    /**
     * Extract table name from INSERT statement.
     */
    public function extractInsertTable(string $sql): ?string
    {
        if (preg_match('/INSERT\s+INTO\s+(?:ONLY\s+)?("[^"]+"|[a-zA-Z_]\w*(?:\."[^"]+"|\.(?:[a-zA-Z_]\w*))?)(?:\s+AS\s+"?(\w+)"?)?/i', $sql, $m) === 1) {
            return $this->unquoteIdentifier($this->stripSchemaPrefix($m[1]));
        }

        return null;
    }

    /**
     * Extract column list from INSERT statement.
     *
     * @return list<string>
     */
    public function extractInsertColumns(string $sql): array
    {
        if (preg_match('/INSERT\s+INTO\s+(?:ONLY\s+)?(?:"[^"]+"|[a-zA-Z_]\w*(?:\."[^"]+"|\.(?:[a-zA-Z_]\w*))?)\s*\(([^)]+)\)\s*(?:VALUES|SELECT|DEFAULT)/i', $sql, $m) === 1) {
            return $this->parseColumnList($m[1]);
        }

        return [];
    }

    /**
     * Extract VALUES rows from INSERT statement.
     *
     * @return list<list<string>>
     */
    public function extractInsertValues(string $sql): array
    {
        $rows = [];
        if (preg_match('/\bVALUES\s*(\(.+)/is', $sql, $m) !== 1) {
            return [];
        }

        $rest = $m[1];
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
     * Check if INSERT has ON CONFLICT clause.
     */
    public function hasOnConflict(string $sql): bool
    {
        return preg_match('/\bON\s+CONFLICT\b/i', $sql) === 1;
    }

    /**
     * Extract ON CONFLICT ... DO UPDATE SET columns and values.
     *
     * @return array{columns: list<string>, values: array<string, string>}
     */
    public function extractOnConflictUpdateColumns(string $sql): array
    {
        $columns = [];
        /** @var array<string, string> $values */
        $values = [];

        if (preg_match('/\bON\s+CONFLICT\b.*?\bDO\s+UPDATE\s+SET\s+(.+?)$/is', $sql, $m) !== 1) {
            return ['columns' => [], 'values' => []];
        }

        $setClause = $m[1];
        $setClause = preg_replace('/\s+WHERE\s+.+$/is', '', $setClause) ?? $setClause;
        $setClause = rtrim($setClause, '; ');

        $assignments = $this->splitByTopLevelComma($setClause);

        foreach ($assignments as $assignment) {
            $assignment = trim($assignment);
            if (preg_match('/^("[^"]+"|[a-zA-Z_]\w*)\s*=\s*(.+)$/s', $assignment, $parts) === 1) {
                $colName = $this->unquoteIdentifier($parts[1]);
                $columns[] = $colName;
                $values[$colName] = trim($parts[2]);
            }
        }

        return ['columns' => $columns, 'values' => $values];
    }

    /**
     * Check if INSERT has a SELECT subquery (INSERT ... SELECT).
     */
    public function hasInsertSelect(string $sql): bool
    {
        $stripped = $this->stripStringLiterals($sql);
        if (preg_match('/\bVALUES\b/i', $stripped) === 1) {
            return false;
        }

        return preg_match('/INSERT\s+INTO\s+.*?\bSELECT\b/is', $stripped) === 1;
    }

    /**
     * Extract the SELECT part from INSERT ... SELECT.
     */
    public function extractInsertSelectSql(string $sql): ?string
    {
        $stripped = $this->stripStringLiterals($sql);
        if (preg_match('/\bSELECT\b/i', $stripped, $m, PREG_OFFSET_CAPTURE) === 1) {
            $offset = $m[0][1];

            return substr($sql, $offset);
        }

        return null;
    }

    /**
     * Extract table name from UPDATE statement.
     */
    public function extractUpdateTable(string $sql): ?string
    {
        if (preg_match('/UPDATE\s+(?:ONLY\s+)?("[^"]+"|[a-zA-Z_]\w*(?:\."[^"]+"|\.(?:[a-zA-Z_]\w*))?)(?:\s+(?:AS\s+)?("[^"]+"|[a-zA-Z_]\w*))?/i', $sql, $m) === 1) {
            return $this->unquoteIdentifier($this->stripSchemaPrefix($m[1]));
        }

        return null;
    }

    /**
     * Extract table alias from UPDATE statement.
     */
    public function extractUpdateAlias(string $sql): ?string
    {
        if (preg_match('/UPDATE\s+(?:ONLY\s+)?(?:"[^"]+"|[a-zA-Z_]\w*(?:\."[^"]+"|\.(?:[a-zA-Z_]\w*))?)\s+(?:AS\s+)?("[^"]+"|[a-zA-Z_]\w*)\s+SET\b/i', $sql, $m) === 1) {
            return $this->unquoteIdentifier($m[1]);
        }

        return null;
    }

    /**
     * Extract SET assignments from UPDATE statement.
     *
     * @return array<string, string> column => value expression
     */
    public function extractUpdateSets(string $sql): array
    {
        if (preg_match('/\bSET\s+(.+?)(?:\s+FROM\s+|\s+WHERE\s+|\s+RETURNING\s+|$)/is', $sql, $m) !== 1) {
            return [];
        }

        $setClause = $m[1];
        $assignments = $this->splitByTopLevelComma($setClause);
        $result = [];

        foreach ($assignments as $assignment) {
            $assignment = trim($assignment);
            if (preg_match('/^("[^"]+"|[a-zA-Z_]\w*)\s*=\s*(.+)$/s', $assignment, $parts) === 1) {
                $colName = $this->unquoteIdentifier($parts[1]);
                $result[$colName] = trim($parts[2]);
            }
        }

        return $result;
    }

    /**
     * Extract WHERE clause from UPDATE or DELETE statement.
     */
    public function extractWhereClause(string $sql): ?string
    {
        $stripped = $this->stripStringLiterals($sql);
        if (preg_match('/\bWHERE\b/i', $stripped, $m, PREG_OFFSET_CAPTURE) !== 1) {
            return null;
        }

        $strippedOffset = $m[0][1];
        $originalOffset = $this->mapStrippedOffsetToOriginal($sql, $stripped, $strippedOffset);

        $whereClause = substr($sql, $originalOffset + strlen('WHERE'));
        $strippedTail = $this->stripStringLiterals($whereClause);
        if (preg_match('/\s+(?:RETURNING|ORDER\s+BY|LIMIT)\b/is', $strippedTail, $tailMatch, PREG_OFFSET_CAPTURE) === 1) {
            $tailOffset = $this->mapStrippedOffsetToOriginal($whereClause, $strippedTail, $tailMatch[0][1]);
            $whereClause = substr($whereClause, 0, $tailOffset);
        }

        return trim($whereClause);
    }

    /**
     * Map an offset in a stripped string back to the corresponding position in the original.
     */
    private function mapStrippedOffsetToOriginal(string $original, string $stripped, int $strippedOffset): int
    {
        if (strlen($original) === strlen($stripped)) {
            return $strippedOffset;
        }

        $origLen = strlen($original);
        $stripLen = strlen($stripped);
        $si = 0;
        $oi = 0;

        while ($si < $strippedOffset && $oi < $origLen && $si < $stripLen) {
            $char = $original[$oi];

            if ($char === "'") {
                $oi++;
                while ($oi < $origLen) {
                    if ($original[$oi] === '\\') {
                        $oi += 2;
                        continue;
                    }
                    if ($original[$oi] === "'") {
                        if ($oi + 1 < $origLen && $original[$oi + 1] === "'") {
                            $oi += 2;
                            continue;
                        }
                        $oi++;
                        break;
                    }
                    $oi++;
                }
                $si += 2;
            } else {
                $oi++;
                $si++;
            }
        }

        return $oi;
    }

    /**
     * Extract FROM clause from UPDATE statement (PostgreSQL extension).
     */
    public function extractUpdateFromClause(string $sql): ?string
    {
        if (preg_match('/\bFROM\s+(.+?)(?:\s+WHERE\s+|\s+RETURNING\s+|$)/is', $sql, $m) === 1) {
            $stripped = $this->stripStringLiterals($sql);
            if (preg_match('/\bSET\b.*?\bFROM\b/is', $stripped) === 1) {
                return trim($m[1]);
            }
        }

        return null;
    }

    /**
     * Extract table name from DELETE statement.
     */
    public function extractDeleteTable(string $sql): ?string
    {
        if (preg_match('/DELETE\s+FROM\s+(?:ONLY\s+)?("[^"]+"|[a-zA-Z_]\w*(?:\."[^"]+"|\.(?:[a-zA-Z_]\w*))?)(?:\s+(?:AS\s+)?("[^"]+"|[a-zA-Z_]\w*))?/i', $sql, $m) === 1) {
            return $this->unquoteIdentifier($this->stripSchemaPrefix($m[1]));
        }

        return null;
    }

    /**
     * Extract table alias from DELETE statement.
     */
    public function extractDeleteAlias(string $sql): ?string
    {
        if (preg_match('/DELETE\s+FROM\s+(?:ONLY\s+)?(?:"[^"]+"|[a-zA-Z_]\w*(?:\."[^"]+"|\.(?:[a-zA-Z_]\w*))?)\s+(?:AS\s+)?("[^"]+"|[a-zA-Z_]\w*)\s+(?:USING\b|WHERE\b|RETURNING\b|$)/i', $sql, $m) === 1) {
            return $this->unquoteIdentifier($m[1]);
        }

        return null;
    }

    /**
     * Extract USING clause from DELETE statement.
     */
    public function extractDeleteUsingClause(string $sql): ?string
    {
        if (preg_match('/\bUSING\s+(.+?)(?:\s+WHERE\s+|\s+RETURNING\s+|$)/is', $sql, $m) === 1) {
            return trim($m[1]);
        }

        return null;
    }

    /**
     * Extract table name from TRUNCATE statement.
     */
    public function extractTruncateTable(string $sql): ?string
    {
        if (preg_match('/TRUNCATE\s+(?:TABLE\s+)?(?:ONLY\s+)?("[^"]+"|[a-zA-Z_]\w*(?:\."[^"]+"|\.(?:[a-zA-Z_]\w*))?)/i', $sql, $m) === 1) {
            return $this->unquoteIdentifier($this->stripSchemaPrefix($m[1]));
        }

        return null;
    }

    /**
     * Extract table name from CREATE TABLE statement.
     */
    public function extractCreateTableName(string $sql): ?string
    {
        if (preg_match('/CREATE\s+(?:TEMPORARY\s+|TEMP\s+|UNLOGGED\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?("[^"]+"|[a-zA-Z_]\w*(?:\."[^"]+"|\.(?:[a-zA-Z_]\w*))?)/i', $sql, $m) === 1) {
            return $this->unquoteIdentifier($this->stripSchemaPrefix($m[1]));
        }

        return null;
    }

    /**
     * Check if CREATE TABLE has IF NOT EXISTS.
     */
    public function hasIfNotExists(string $sql): bool
    {
        return preg_match('/CREATE\s+(?:TEMPORARY\s+|TEMP\s+|UNLOGGED\s+)?TABLE\s+IF\s+NOT\s+EXISTS\b/i', $sql) === 1;
    }

    /**
     * Check if CREATE TABLE has AS SELECT.
     */
    public function hasCreateTableAsSelect(string $sql): bool
    {
        return preg_match('/CREATE\s+(?:TEMPORARY\s+|TEMP\s+|UNLOGGED\s+)?TABLE\s+.*?\bAS\s+SELECT\b/is', $sql) === 1;
    }

    /**
     * Extract the SELECT SQL from CREATE TABLE ... AS SELECT.
     */
    public function extractCreateTableSelectSql(string $sql): ?string
    {
        if (preg_match('/\bAS\s+(SELECT\b.+)$/is', $sql, $m) === 1) {
            return trim($m[1]);
        }

        return null;
    }

    /**
     * Check if CREATE TABLE has LIKE clause.
     */
    public function hasCreateTableLike(string $sql): bool
    {
        return preg_match('/CREATE\s+(?:TEMPORARY\s+|TEMP\s+|UNLOGGED\s+)?TABLE\s+.*?\(\s*LIKE\s+/is', $sql) === 1;
    }

    /**
     * Extract the LIKE source table name.
     */
    public function extractCreateTableLikeSource(string $sql): ?string
    {
        if (preg_match('/\(\s*LIKE\s+("[^"]+"|[a-zA-Z_]\w*(?:\."[^"]+"|\.(?:[a-zA-Z_]\w*))?)/i', $sql, $m) === 1) {
            return $this->unquoteIdentifier($this->stripSchemaPrefix($m[1]));
        }

        return null;
    }

    /**
     * Extract table name from DROP TABLE statement.
     */
    public function extractDropTableName(string $sql): ?string
    {
        if (preg_match('/DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?("[^"]+"|[a-zA-Z_]\w*(?:\."[^"]+"|\.(?:[a-zA-Z_]\w*))?)/i', $sql, $m) === 1) {
            return $this->unquoteIdentifier($this->stripSchemaPrefix($m[1]));
        }

        return null;
    }

    /**
     * Check if DROP TABLE has IF EXISTS.
     */
    public function hasDropTableIfExists(string $sql): bool
    {
        return preg_match('/DROP\s+TABLE\s+IF\s+EXISTS\b/i', $sql) === 1;
    }

    /**
     * Extract table name from ALTER TABLE statement.
     */
    public function extractAlterTableName(string $sql): ?string
    {
        if (preg_match('/ALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?:ONLY\s+)?("[^"]+"|[a-zA-Z_]\w*(?:\."[^"]+"|\.(?:[a-zA-Z_]\w*))?)/i', $sql, $m) === 1) {
            return $this->unquoteIdentifier($this->stripSchemaPrefix($m[1]));
        }

        return null;
    }

    /**
     * Unquote a PostgreSQL identifier (remove double quotes).
     */
    public function unquoteIdentifier(string $identifier): string
    {
        if (str_starts_with($identifier, '"') && str_ends_with($identifier, '"')) {
            $inner = substr($identifier, 1, -1);

            return str_replace('""', '"', $inner);
        }

        return $identifier;
    }

    /**
     * Strip schema prefix from a potentially schema-qualified name.
     * "public"."users" -> "users", public.users -> users
     */
    public function stripSchemaPrefix(string $name): string
    {
        if (preg_match('/^"[^"]+"\.(.+)$/', $name, $m) === 1) {
            return $m[1];
        }
        if (preg_match('/^[a-zA-Z_]\w*\.(.+)$/', $name, $m) === 1) {
            return $m[1];
        }

        return $name;
    }

    /**
     * Extract table names referenced in a SELECT statement.
     *
     * @return list<string>
     */
    public function extractSelectTableNames(string $sql): array
    {
        $tables = [];
        $stripped = $this->stripStringLiterals($sql);

        if (preg_match_all('/\bFROM\s+(.+?)(?:\s+WHERE\b|\s+GROUP\b|\s+HAVING\b|\s+ORDER\b|\s+LIMIT\b|\s+OFFSET\b|\s+UNION\b|\s+INTERSECT\b|\s+EXCEPT\b|\s+FOR\b|\s*;|\s*$)/is', $stripped, $fromMatches) > 0) {
            foreach ($fromMatches[1] as $fromClause) {
                $tables = array_merge($tables, $this->extractTableRefsFromClause($fromClause));
            }
        }

        if (preg_match_all('/\bJOIN\s+("[^"]+"|[a-zA-Z_]\w*(?:\."[^"]+"|\.(?:[a-zA-Z_]\w*))?)/i', $stripped, $joinMatches) > 0) {
            foreach ($joinMatches[1] as $joinTable) {
                $tables[] = $this->unquoteIdentifier($this->stripSchemaPrefix($joinTable));
            }
        }

        return array_values(array_unique($tables));
    }

    /**
     * @return list<string>
     */
    private function extractTableRefsFromClause(string $clause): array
    {
        $tables = [];
        $parts = $this->splitByTopLevelComma($clause);

        foreach ($parts as $part) {
            $part = trim($part);
            if (str_starts_with($part, '(')) {
                continue;
            }
            $part = preg_replace('/\b(?:INNER|LEFT|RIGHT|FULL|CROSS|NATURAL)\s+(?:OUTER\s+)?JOIN\b.*/is', '', $part) ?? $part;
            $part = preg_replace('/\bJOIN\b.*/is', '', $part) ?? $part;
            $part = trim($part);

            if (preg_match('/^("[^"]+"|[a-zA-Z_]\w*(?:\."[^"]+"|\.(?:[a-zA-Z_]\w*))?)(?:\s+(?:AS\s+)?("[^"]+"|[a-zA-Z_]\w*))?/i', $part, $m) === 1) {
                $tableName = $this->unquoteIdentifier($this->stripSchemaPrefix($m[1]));
                if ($tableName !== '' && !$this->isSqlKeyword($tableName)) {
                    $tables[] = $tableName;
                }
            }
        }

        return $tables;
    }

    private function classifyWithStatement(string $sql): ?string
    {
        $stripped = $this->stripStringLiterals($sql);
        $len = strlen($stripped);
        $depth = 0;
        $seenCteBody = false;
        $inQuote = false;

        for ($i = 0; $i < $len; $i++) {
            $char = $stripped[$i];

            if ($inQuote) {
                if ($char === '"') {
                    $inQuote = false;
                }
                continue;
            }

            if ($char === '"') {
                $inQuote = true;
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

            $prev = $i > 0 ? $stripped[$i - 1] : ' ';
            if (ctype_alpha($prev) || $prev === '_') {
                continue;
            }

            $j = $i;
            while ($j < $len && (ctype_alpha($stripped[$j]) || $stripped[$j] === '_')) {
                $j++;
            }

            $keyword = strtoupper(substr($stripped, $i, $j - $i));

            $result = match ($keyword) {
                'SELECT' => 'SELECT',
                'INSERT' => 'INSERT',
                'UPDATE' => 'UPDATE',
                'DELETE' => 'DELETE',
                default => null,
            };

            if ($result !== null) {
                return $result;
            }

            $i = $j - 1;
        }

        return null;
    }

    private function classifySimpleStatement(string $sql): ?string
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

        if (preg_match('/^(?:BEGIN|START\s+TRANSACTION|COMMIT|ROLLBACK|SAVEPOINT|RELEASE\s+SAVEPOINT|SET\s+TRANSACTION)\b/i', $trimmed) === 1) {
            return 'TCL';
        }

        return null;
    }

    private function stripLeadingComments(string $sql): string
    {
        $result = $sql;
        $changed = true;
        while ($changed) {
            $changed = false;
            $result = ltrim($result);
            if (str_starts_with($result, '--')) {
                $pos = strpos($result, "\n");
                $result = $pos !== false ? substr($result, $pos + 1) : '';
                $changed = true;
            }
            if (str_starts_with($result, '/*')) {
                $pos = strpos($result, '*/');
                $result = $pos !== false ? substr($result, $pos + 2) : '';
                $changed = true;
            }
        }

        return $result;
    }

    /**
     * @return list<string>
     */
    private function parseColumnList(string $columnStr): array
    {
        $columns = [];
        $parts = explode(',', $columnStr);
        foreach ($parts as $part) {
            $col = trim($part);
            $col = $this->unquoteIdentifier($col);
            if ($col !== '') {
                $columns[] = $col;
            }
        }

        return $columns;
    }

    /**
     * @return array{items: list<string>, end: int}|null
     */
    private function extractParenthesizedList(string $str, int $start): ?array
    {
        if (!isset($str[$start]) || $str[$start] !== '(') {
            return null;
        }

        $items = [];
        $current = '';
        $depth = 0;
        $len = strlen($str);
        $inSingleQuote = false;

        for ($pos = $start; $pos < $len; $pos++) {
            $char = $str[$pos];

            if ($inSingleQuote) {
                $current .= $char;
                if ($char === "'" && isset($str[$pos + 1]) && $str[$pos + 1] === "'") {
                    $current .= "'";
                    $pos++;
                } elseif ($char === "'") {
                    $inSingleQuote = false;
                }
                continue;
            }

            if ($char === "'") {
                $current .= $char;
                $inSingleQuote = true;
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

            if ($char === ',' && $depth === 1) {
                $items[] = trim($current);
                $current = '';
                continue;
            }

            $current .= $char;
        }

        return null;
    }

    /**
     * Split a string by commas, respecting parentheses and quotes.
     *
     * @return list<string>
     */
    private function splitByTopLevelComma(string $str): array
    {
        $parts = [];
        $current = '';
        $depth = 0;
        $inSingleQuote = false;
        $inDoubleQuote = false;
        $len = strlen($str);

        for ($i = 0; $i < $len; $i++) {
            $char = $str[$i];

            if ($inSingleQuote) {
                $current .= $char;
                if ($char === "'" && isset($str[$i + 1]) && $str[$i + 1] === "'") {
                    $current .= "'";
                    $i++;
                } elseif ($char === "'") {
                    $inSingleQuote = false;
                }
                continue;
            }

            if ($inDoubleQuote) {
                $current .= $char;
                if ($char === '"' && isset($str[$i + 1]) && $str[$i + 1] === '"') {
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

    private function stripStringLiterals(string $sql): string
    {
        $result = preg_replace("/E?'(?:[^'\\\\]|\\\\.)*'/", "''", $sql);
        $result = $result !== null ? $result : $sql;
        $result = preg_replace('/\$\w*\$.*?\$\w*\$/s', "''", $result);

        return $result !== null ? $result : $sql;
    }

    private function extractDollarTag(string $sql, int $pos): ?string
    {
        $len = strlen($sql);
        $i = $pos + 1;

        if ($i < $len && $sql[$i] === '$') {
            return '';
        }

        $tag = '';
        while ($i < $len && (ctype_alnum($sql[$i]) || $sql[$i] === '_')) {
            $tag .= $sql[$i];
            $i++;
        }

        if ($i < $len && $sql[$i] === '$' && $tag !== '') {
            return $tag;
        }

        return null;
    }

    private function isSqlKeyword(string $word): bool
    {
        $upper = strtoupper($word);

        return in_array($upper, [
            'SELECT', 'FROM', 'WHERE', 'AND', 'OR', 'NOT', 'IN', 'EXISTS',
            'BETWEEN', 'LIKE', 'IS', 'NULL', 'TRUE', 'FALSE', 'AS', 'ON',
            'JOIN', 'INNER', 'LEFT', 'RIGHT', 'FULL', 'OUTER', 'CROSS',
            'NATURAL', 'USING', 'ORDER', 'BY', 'GROUP', 'HAVING', 'LIMIT',
            'OFFSET', 'UNION', 'ALL', 'INTERSECT', 'EXCEPT', 'INSERT',
            'INTO', 'VALUES', 'UPDATE', 'SET', 'DELETE', 'WITH', 'RECURSIVE',
            'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'CAST', 'RETURNING',
        ], true);
    }
}
