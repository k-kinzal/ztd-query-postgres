<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres;

use ZtdQuery\Schema\ColumnType;
use ZtdQuery\Schema\IdentityGenerationStrategy;
use ZtdQuery\Platform\SchemaParser;
use ZtdQuery\Schema\TableDefinition;
use ZtdQuery\Sql\SqlToken;
use ZtdQuery\Sql\SqlTokenKind;
use ZtdQuery\Sql\SqlTokenStream;

/**
 * PostgreSQL implementation of SchemaParser.
 *
 * Parses CREATE TABLE statements into structured schema metadata.
 */
final class PgSqlSchemaParser implements SchemaParser
{
    /**
     * {@inheritDoc}
     */
    public function parse(string $createTableSql): ?TableDefinition
    {
        $body = $this->tableBody($createTableSql);
        if ($body === null) {
            return null;
        }

        $columns = [];
        $columnTypes = [];
        /** @var array<string, ColumnType> $typedColumns */
        $typedColumns = [];
        $columnDefaults = [];
        $identityStrategies = [];
        $generatedExpressions = [];
        $primaryKeys = [];
        $notNullColumns = [];
        /** @var array<string, list<string>> $uniqueConstraints */
        $uniqueConstraints = [];
        $uniqueIndex = 0;
        $foreignKeys = (new PgSqlForeignKeyDefinitionParser())->parseCreateTable($createTableSql);

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

            if ($columnDef['default'] !== null && !self::isSequenceDefault($columnDef['default'])) {
                $columnDefaults[$columnDef['name']] = $columnDef['default'];
            }
            if ($columnDef['identity']) {
                $identityStrategies[$columnDef['name']] = IdentityGenerationStrategy::Sequence;
            }
            if ($columnDef['generatedExpression'] !== null) {
                $generatedExpressions[$columnDef['name']] = $columnDef['generatedExpression'];
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
            $columnDefaults,
            $identityStrategies,
            $generatedExpressions,
            $foreignKeys,
            partitionKey: (new PgSqlPartitionParser())->parseKey($createTableSql),
        );
    }

    private function tableBody(string $sql): ?string
    {
        $stream = SqlTokenStream::tokenize($sql, PgSqlLexerProfile::create());
        $tokens = $stream->significantTokens();
        $create = $tokens[0] ?? null;
        if (!$create instanceof SqlToken || !$create->isKeyword('CREATE')) {
            return null;
        }

        $tableIndex = null;
        foreach ($tokens as $index => $token) {
            if ($token->isTopLevel() && $token->isKeyword('TABLE')) {
                $tableIndex = $index;
            }
        }
        if ($tableIndex === null) {
            return null;
        }

        $index = $tableIndex + 1;
        $candidate = $tokens[$index] ?? null;
        if (!$candidate instanceof SqlToken) {
            return null;
        }
        if ($candidate->isKeyword('IF')) {
            $not = $tokens[$index + 1] ?? null;
            $exists = $tokens[$index + 2] ?? null;
            if (!$not instanceof SqlToken || !$not->isKeyword('NOT')) {
                return null;
            }
            if (!$exists instanceof SqlToken || !$exists->isKeyword('EXISTS')) {
                return null;
            }
            $index += 3;
        }
        $identifier = $this->qualifiedIdentifierAt($stream, $tokens, $index);
        if ($identifier === null) {
            return null;
        }

        $open = $tokens[$identifier['next']] ?? null;
        if (!$open instanceof SqlToken) {
            return null;
        }
        if (!$this->isSymbol($open, '(')) {
            return null;
        }

        foreach ($tokens as $token) {
            if ($this->isSymbol($token, ')') && $token->depth === $open->depth) {
                return substr($sql, $open->endOffset(), $token->offset - $open->endOffset());
            }
        }

        return null;
    }

    /**
     * @param list<SqlToken> $tokens
     * @return array{name: string, next: int}|null
     */
    private function qualifiedIdentifierAt(SqlTokenStream $stream, array $tokens, int $index): ?array
    {
        $token = $tokens[$index] ?? null;
        if (!$token instanceof SqlToken) {
            return null;
        }
        if (!in_array($token->kind, [SqlTokenKind::Word, SqlTokenKind::QuotedIdentifier], true)) {
            return null;
        }
        $identifier = $stream->identifierAt($index);
        if ($identifier === null) {
            return null;
        }

        $dot = $tokens[$identifier['next']] ?? null;
        while ($dot instanceof SqlToken && $this->isSymbol($dot, '.')) {
            $component = $this->qualifiedIdentifierAt($stream, $tokens, $identifier['next'] + 1);
            if ($component === null) {
                return null;
            }
            $identifier = $component;
            $dot = $tokens[$identifier['next']] ?? null;
        }

        return $identifier;
    }

    private function isSymbol(SqlToken $token, string $symbol): bool
    {
        return $token->kind === SqlTokenKind::Symbol && $token->text === $symbol;
    }

    private static function isSequenceDefault(string $expression): bool
    {
        foreach (SqlTokenStream::tokenize($expression, PgSqlLexerProfile::create())->significantTokens() as $token) {
            if ($token->text === '(') {
                continue;
            }

            return $token->isKeyword('NEXTVAL');
        }

        return false;
    }

    /**
     * @return array{name: string, type: string, columnType: ColumnType, notNull: bool, primaryKey: bool, unique: bool, default: string|null, identity: bool, generatedExpression: string|null}|null
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
        $default = SqlTokenStream::tokenize($afterType, PgSqlLexerProfile::create())->topLevelClause(
            ['DEFAULT'],
            [
                ['NOT', 'NULL'], ['PRIMARY', 'KEY'], ['UNIQUE'], ['CHECK'],
                ['REFERENCES'], ['COLLATE'], ['CONSTRAINT'], ['GENERATED'], ['DEFERRABLE'],
            ],
        );
        $identity = self::isSerialType($nativeType)
            || ($default !== null && self::isSequenceDefault($default))
            || self::hasGeneratedIdentity($afterType);
        $generatedExpression = null;
        if (!$identity) {
            $generatedExpression = SqlTokenStream::tokenize($afterType, PgSqlLexerProfile::create())->topLevelClause(
                ['GENERATED', 'ALWAYS', 'AS'],
                [['STORED']],
            );
            if ($generatedExpression === '') {
                $generatedExpression = null;
            }
        }

        $normalizedType = str_contains($nativeType, '"') ? $nativeType : strtoupper($nativeType);
        $columnType = (new PgSqlColumnTypeMapper())->map($normalizedType);

        return [
            'name' => $name,
            'type' => $normalizedType,
            'columnType' => $columnType,
            'notNull' => $notNull,
            'primaryKey' => $primaryKey,
            'unique' => $unique,
            'default' => $default,
            'identity' => $identity,
            'generatedExpression' => $generatedExpression,
        ];
    }

    private static function isSerialType(string $nativeType): bool
    {
        foreach (['SMALLSERIAL', 'SERIAL', 'BIGSERIAL'] as $serialType) {
            if (strcasecmp($nativeType, $serialType) === 0) {
                return true;
            }
        }

        return false;
    }

    private static function hasGeneratedIdentity(string $constraints): bool
    {
        $tokens = SqlTokenStream::tokenize($constraints, PgSqlLexerProfile::create())->significantTokens();
        $sequences = [
            ['GENERATED', 'ALWAYS', 'AS', 'IDENTITY'],
            ['GENERATED', 'BY', 'DEFAULT', 'AS', 'IDENTITY'],
        ];
        foreach ($tokens as $index => $token) {
            foreach ($sequences as $sequence) {
                foreach ($sequence as $relative => $keyword) {
                    $candidate = $tokens[$index + $relative] ?? null;
                    if ($candidate === null || !$candidate->isTopLevel() || !$candidate->isKeyword($keyword)) {
                        continue 2;
                    }
                }

                return true;
            }
        }

        return false;
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

        $quotedType = $this->extractQuotedType($str);
        if ($quotedType !== null) {
            return $quotedType;
        }

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

    /** @return array{type: string, rest: string}|null */
    private function extractQuotedType(string $str): ?array
    {
        $tokens = SqlTokenStream::tokenize($str, PgSqlLexerProfile::create())->significantTokens();
        $first = $tokens[0] ?? null;
        if ($first === null) {
            return null;
        }
        if ($first->kind !== SqlTokenKind::QuotedIdentifier) {
            return null;
        }

        $last = $first;
        $index = 1;
        $separator = $tokens[$index] ?? null;
        if ($separator !== null && $separator->text === '.') {
            $index++;
            $qualifiedName = $tokens[$index] ?? null;
            if ($qualifiedName === null) {
                return null;
            }
            if (!self::isTypeIdentifier($qualifiedName)) {
                return null;
            }
            if ($qualifiedName->kind === SqlTokenKind::Word) {
                if (in_array(strtoupper($qualifiedName->text), self::CONSTRAINT_KEYWORDS, true)) {
                    return null;
                }
            }
            $last = $qualifiedName;
            $index++;
        }

        while (isset($tokens[$index]) && $tokens[$index]->text === '[') {
            $index++;
            $arrayClosing = $tokens[$index] ?? null;
            if ($arrayClosing === null) {
                return null;
            }
            if ($arrayClosing->text !== ']') {
                return null;
            }
            $last = $arrayClosing;
            $index++;
        }

        return [
            'type' => substr($str, 0, $last->endOffset()),
            'rest' => substr($str, $last->endOffset()),
        ];
    }

    private static function isTypeIdentifier(SqlToken $token): bool
    {
        return in_array($token->kind, [SqlTokenKind::Word, SqlTokenKind::QuotedIdentifier], true);
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
