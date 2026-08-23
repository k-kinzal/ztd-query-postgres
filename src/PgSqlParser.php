<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres;

use ZtdQuery\Sql\SqlToken;
use ZtdQuery\Sql\SqlTokenKind;
use ZtdQuery\Sql\SqlTokenStream;

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
     * @return 'SELECT'|'INSERT'|'UPDATE'|'DELETE'|'MERGE'|'TRUNCATE'|'CREATE_TABLE'|'DROP_TABLE'|'ALTER_TABLE'|'DO'|'TCL'|null
     */
    public function classifyStatement(string $sql): ?string
    {
        $trimmed = $this->maskComments($sql);

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
        return SqlTokenStream::tokenize($sql, PgSqlLexerProfile::create())->splitStatements();
    }

    /**
     * Extract table name from INSERT statement.
     */
    public function extractInsertTable(string $sql): ?string
    {
        $sql = $this->maskComments($sql);
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
        $sql = $this->maskComments($sql);
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
        $sql = $this->maskComments($sql);
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
     * Check if INSERT has ON CONFLICT clause.
     */
    public function hasOnConflict(string $sql): bool
    {
        $sql = $this->maskComments($sql);
        return preg_match('/\bON\s+CONFLICT\b/i', $sql) === 1;
    }

    public function extractOnConflictTarget(string $sql): ?PgSqlConflictTarget
    {
        $tokens = SqlTokenStream::tokenize($sql, PgSqlLexerProfile::create())->significantTokens();
        $conflict = self::findOnConflictTargetStart($tokens);
        if ($conflict === null) {
            return null;
        }

        $next = $tokens[$conflict] ?? null;
        if ($next === null) {
            return null;
        }
        if ($next->isKeyword('DO')) {
            return new PgSqlConflictTarget(false);
        }
        if ($next->isKeyword('ON')) {
            $constraintKeyword = $tokens[$conflict + 1] ?? null;
            if ($constraintKeyword === null) {
                return null;
            }
            if (!$constraintKeyword->isKeyword('CONSTRAINT')) {
                return null;
            }
            $constraintToken = $tokens[$conflict + 2] ?? null;
            if ($constraintToken === null) {
                return null;
            }
            $identifier = SqlTokenStream::tokenize($constraintToken->text, PgSqlLexerProfile::create())->identifierAt();
            if ($identifier === null) {
                return null;
            }

            return new PgSqlConflictTarget(true, constraint: $identifier['name']);
        }
        if ($next->text !== '(') {
            return null;
        }

        $closing = self::findTopLevelSymbol($tokens, $conflict, ')');
        if ($closing === null) {
            return null;
        }

        $columnSql = substr(
            $sql,
            $next->endOffset(),
            $tokens[$closing]->offset - $next->endOffset(),
        );
        $columns = [];
        foreach (SqlTokenStream::tokenize($columnSql, PgSqlLexerProfile::create())->splitTopLevel() as $part) {
            $stream = SqlTokenStream::tokenize($part, PgSqlLexerProfile::create());
            $identifier = $stream->identifierAt();
            if ($identifier === null) {
                return null;
            }
            if ($identifier['next'] !== count($stream->significantTokens())) {
                return null;
            }
            $columns[] = $identifier['name'];
        }

        $do = self::findTopLevelKeyword($tokens, $closing, 'DO');
        if ($do === null) {
            return null;
        }

        $predicate = null;
        $afterColumns = $tokens[$closing + 1] ?? null;
        if ($afterColumns !== null) {
            if ($afterColumns->isKeyword('WHERE')) {
                $predicate = trim(substr(
                    $sql,
                    $afterColumns->endOffset(),
                    $tokens[$do]->offset - $afterColumns->endOffset(),
                ));
                if ($predicate === '') {
                    return null;
                }
            }
        }

        return new PgSqlConflictTarget(true, $columns, $predicate);
    }

    /**
     * @param list<SqlToken> $tokens
     */
    private static function findOnConflictTargetStart(array $tokens): ?int
    {
        foreach ($tokens as $index => $token) {
            if (!$token->isTopLevel()) {
                continue;
            }
            if (!$token->isKeyword('ON')) {
                continue;
            }
            $following = $tokens[$index + 1] ?? $token;
            if ($following->isKeyword('CONFLICT')) {
                return $index + 2;
            }
        }

        return null;
    }

    /**
     * @param list<SqlToken> $tokens
     */
    private static function findTopLevelSymbol(array $tokens, int $start, string $symbol): ?int
    {
        for ($index = $start, $count = count($tokens); $index < $count; $index++) {
            $token = $tokens[$index];
            if (!$token->isTopLevel()) {
                continue;
            }
            if ($token->kind !== SqlTokenKind::Symbol) {
                continue;
            }
            if ($token->text === $symbol) {
                return $index;
            }
        }

        return null;
    }

    /**
     * @param list<SqlToken> $tokens
     */
    private static function findTopLevelKeyword(array $tokens, int $start, string $keyword): ?int
    {
        for ($index = $start, $count = count($tokens); $index < $count; $index++) {
            $token = $tokens[$index];
            if ($token->isTopLevel() && $token->isKeyword($keyword)) {
                return $index;
            }
        }

        return null;
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

        $action = SqlTokenStream::tokenize($sql, PgSqlLexerProfile::create())->topLevelClause(['DO'], [['RETURNING']]);
        if ($action === null) {
            return ['columns' => [], 'values' => []];
        }
        $actionStream = SqlTokenStream::tokenize($action, PgSqlLexerProfile::create());
        if ($actionStream->firstTopLevelKeyword() !== 'UPDATE') {
            return ['columns' => [], 'values' => []];
        }
        $setClause = $actionStream->topLevelClause(['SET'], [['WHERE']]);
        if ($setClause === null) {
            return ['columns' => [], 'values' => []];
        }
        $setClause = rtrim($setClause, '; ');

        $assignments = SqlTokenStream::tokenize($setClause, PgSqlLexerProfile::create())->splitTopLevel();

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

    public function extractOnConflictUpdateWhere(string $sql): ?string
    {
        return SqlTokenStream::tokenize($sql, PgSqlLexerProfile::create())->topLevelClauseAfter(
            ['DO', 'UPDATE', 'SET'],
            ['WHERE'],
            [['RETURNING']],
        );
    }

    /**
     * Check if INSERT has a SELECT subquery (INSERT ... SELECT).
     */
    public function hasInsertSelect(string $sql): bool
    {
        $sql = $this->maskComments($sql);
        $source = $this->findInsertSourceClause($sql);

        return $source !== null && $source['keyword'] === 'SELECT';
    }

    /**
     * Extract the SELECT part from INSERT ... SELECT.
     */
    public function extractInsertSelectSql(string $sql): ?string
    {
        $sql = $this->maskComments($sql);
        $source = $this->findInsertSourceClause($sql);
        if ($source !== null && $source['keyword'] === 'SELECT') {
            return substr($sql, $source['offset']);
        }

        return null;
    }

    /**
     * Extract table name from UPDATE statement.
     */
    public function extractUpdateTable(string $sql): ?string
    {
        $sql = $this->maskComments($sql);
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
        $sql = $this->maskComments($sql);
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
        $setClause = SqlTokenStream::tokenize($sql, PgSqlLexerProfile::create())->topLevelClause(
            ['SET'],
            [['FROM'], ['WHERE'], ['RETURNING']],
        );
        if ($setClause === null) {
            return [];
        }

        $assignments = SqlTokenStream::tokenize($setClause, PgSqlLexerProfile::create())->splitTopLevel();
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
        return SqlTokenStream::tokenize($sql, PgSqlLexerProfile::create())->topLevelClause(
            ['WHERE'],
            [['RETURNING'], ['ORDER', 'BY'], ['LIMIT']],
        );
    }

    /**
     * Extract FROM clause from UPDATE statement (PostgreSQL extension).
     */
    public function extractUpdateFromClause(string $sql): ?string
    {
        return SqlTokenStream::tokenize($sql, PgSqlLexerProfile::create())->topLevelClause(
            ['FROM'],
            [['WHERE'], ['RETURNING']],
        );
    }

    /**
     * Extract table name from DELETE statement.
     */
    public function extractDeleteTable(string $sql): ?string
    {
        $sql = $this->maskComments($sql);
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
        $sql = $this->maskComments($sql);
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
        return SqlTokenStream::tokenize($sql, PgSqlLexerProfile::create())->topLevelClause(
            ['USING'],
            [['WHERE'], ['RETURNING']],
        );
    }

    /**
     * Extract table name from TRUNCATE statement.
     */
    public function extractTruncateTable(string $sql): ?string
    {
        return $this->extractTruncateTables($sql)[0] ?? null;
    }

    /** @return list<string> */
    public function extractTruncateTables(string $sql): array
    {
        $stream = SqlTokenStream::tokenize($sql, PgSqlLexerProfile::create());
        $tokens = $stream->significantTokens();
        if ($stream->firstTopLevelKeyword() !== 'TRUNCATE') {
            return [];
        }

        $index = 1;
        $tableKeyword = $tokens[$index] ?? null;
        if ($tableKeyword !== null && $tableKeyword->isKeyword('TABLE')) {
            $index++;
        }

        $tableNames = [];
        while (isset($tokens[$index])) {
            if ($tokens[$index]->isKeyword('ONLY')) {
                $index++;
            }

            $identifier = $this->truncateIdentifierAt($stream, $index);
            if ($identifier === null) {
                break;
            }
            $tableName = $identifier['name'];
            $index = $identifier['next'];

            while (($tokens[$index] ?? null)?->text === '.') {
                $identifier = $this->truncateIdentifierAt($stream, $index + 1);
                if ($identifier === null) {
                    break 2;
                }
                $tableName = $identifier['name'];
                $index = $identifier['next'];
            }

            if (($tokens[$index] ?? null)?->text === '*') {
                $index++;
            }
            $tableNames[] = $tableName;

            if (($tokens[$index] ?? null)?->text !== ',') {
                break;
            }
            $index++;
        }

        return $tableNames;
    }

    /** @return array{name: string, next: int}|null */
    private function truncateIdentifierAt(SqlTokenStream $stream, int $index): ?array
    {
        $tokens = $stream->significantTokens();
        $prefix = $tokens[$index] ?? null;
        if ($prefix === null) {
            return null;
        }
        if ($prefix->isKeyword('U')) {
            $ampersand = $tokens[$index + 1] ?? null;
            if ($ampersand !== null && $ampersand->text === '&') {
                $quotedIdentifier = $tokens[$index + 2] ?? null;
                if ($quotedIdentifier === null || $quotedIdentifier->kind !== SqlTokenKind::QuotedIdentifier) {
                    return null;
                }

                return [
                    'name' => $this->unquoteIdentifier($quotedIdentifier->text),
                    'next' => $index + 3,
                ];
            }
        }

        return $stream->identifierAt($index);
    }

    /**
     * Extract table name from CREATE TABLE statement.
     */
    public function extractCreateTableName(string $sql): ?string
    {
        $sql = $this->maskComments($sql);
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
        $sql = $this->maskComments($sql);
        return preg_match('/CREATE\s+(?:TEMPORARY\s+|TEMP\s+|UNLOGGED\s+)?TABLE\s+IF\s+NOT\s+EXISTS\b/i', $sql) === 1;
    }

    /**
     * Check if CREATE TABLE has AS SELECT.
     */
    public function hasCreateTableAsSelect(string $sql): bool
    {
        $sql = $this->maskComments($sql);
        return preg_match('/CREATE\s+(?:TEMPORARY\s+|TEMP\s+|UNLOGGED\s+)?TABLE\s+.*?\bAS\s+SELECT\b/is', $sql) === 1;
    }

    /**
     * Extract the SELECT SQL from CREATE TABLE ... AS SELECT.
     */
    public function extractCreateTableSelectSql(string $sql): ?string
    {
        $sql = $this->maskComments($sql);
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
        $sql = $this->maskComments($sql);
        return preg_match('/CREATE\s+(?:TEMPORARY\s+|TEMP\s+|UNLOGGED\s+)?TABLE\s+.*?\(\s*LIKE\s+/is', $sql) === 1;
    }

    /**
     * Extract the LIKE source table name.
     */
    public function extractCreateTableLikeSource(string $sql): ?string
    {
        $sql = $this->maskComments($sql);
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
        $sql = $this->maskComments($sql);
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
        $sql = $this->maskComments($sql);
        return preg_match('/DROP\s+TABLE\s+IF\s+EXISTS\b/i', $sql) === 1;
    }

    /**
     * Extract table name from ALTER TABLE statement.
     */
    public function extractAlterTableName(string $sql): ?string
    {
        $sql = $this->maskComments($sql);
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
        return (new PgSqlSelectRelationParser())->tableNames($sql);
    }

    /** @return 'SELECT'|'INSERT'|'UPDATE'|'DELETE'|'MERGE'|null */
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
                'MERGE' => 'MERGE',
                default => null,
            };

            if ($result !== null) {
                return $result;
            }

            $i = $j - 1;
        }

        return null;
    }

    /** @return 'SELECT'|'INSERT'|'UPDATE'|'DELETE'|'MERGE'|'TRUNCATE'|'CREATE_TABLE'|'DROP_TABLE'|'ALTER_TABLE'|'DO'|'TCL'|null */
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
        $bracketDepth = 0;
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

            if ($char === '[') {
                $bracketDepth++;
                $current .= $char;
                continue;
            }

            if ($char === ']' && $bracketDepth > 0) {
                $bracketDepth--;
                $current .= $char;
                continue;
            }

            if ($char === ',' && $depth === 1 && $bracketDepth === 0) {
                $items[] = trim($current);
                $current = '';
                continue;
            }

            $current .= $char;
        }

        return null;
    }

    private function maskComments(string $sql): string
    {
        return PostgreSqlLexicalMasker::maskComments($sql);
    }

    private function stripStringLiterals(string $sql): string
    {
        return PostgreSqlLexicalMasker::maskStringLiterals($sql);
    }

    /**
     * @return array{keyword: string, offset: int}|null
     */
    private function findInsertSourceClause(string $sql): ?array
    {
        $searchable = $this->stripStringLiterals($sql);
        $length = strlen($searchable);
        $depth = 0;
        $foundInsert = false;

        for ($i = 0; $i < $length; $i++) {
            if ($searchable[$i] === '"') {
                for ($i++; $i < $length; $i++) {
                    if ($searchable[$i] !== '"') {
                        continue;
                    }
                    break;
                }
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

}
