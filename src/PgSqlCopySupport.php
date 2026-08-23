<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres;

use ZtdQuery\Platform\CopySupport;
use ZtdQuery\Platform\CopyTarget;
use ZtdQuery\Schema\TableDefinition;
use ZtdQuery\Sql\SqlToken;
use ZtdQuery\Sql\SqlTokenKind;
use ZtdQuery\Sql\SqlTokenStream;

final class PgSqlCopySupport implements CopySupport
{
    public function tableName(string $relation): string
    {
        $parts = $this->relationParts($relation);

        return $parts[count($parts) - 1];
    }

    public function target(string $relation, ?string $fields, TableDefinition $definition): CopyTarget
    {
        $columns = $this->columns($fields, $definition);
        if ($columns === []) {
            throw new \ValueError('PostgreSQL COPY requires at least one non-generated column.');
        }

        return new CopyTarget($this->relationParts($relation), $columns);
    }

    public function selectSql(CopyTarget $target): string
    {
        return sprintf(
            'SELECT %s FROM %s',
            $this->columnListSql($target->columns),
            $this->relationSql($target),
        );
    }

    public function insertSql(CopyTarget $target, int $rowCount, bool $overrideSystemValue): string
    {
        if ($rowCount < 1) {
            throw new \ValueError('PostgreSQL COPY INSERT requires at least one row.');
        }

        $parameter = 1;
        $valueRows = [];
        for ($row = 0; $row < $rowCount; $row++) {
            $placeholders = [];
            foreach ($target->columns as $_column) {
                $placeholders[] = '$' . $parameter++;
            }
            $valueRows[] = '(' . implode(', ', $placeholders) . ')';
        }

        return sprintf(
            'INSERT INTO %s (%s)%s VALUES %s',
            $this->relationSql($target),
            $this->columnListSql($target->columns),
            $overrideSystemValue ? ' OVERRIDING SYSTEM VALUE' : '',
            implode(', ', $valueRows),
        );
    }

    public function isCopyStatement(string $sql): bool
    {
        return SqlTokenStream::tokenize($sql, PgSqlLexerProfile::create())->firstTopLevelKeyword() === 'COPY';
    }

    /** @return non-empty-list<string> */
    private function relationParts(string $tableName): array
    {
        $parts = SqlTokenStream::tokenize($tableName, PgSqlLexerProfile::create())->splitTopLevel('.');
        if ($parts === []) {
            throw new \ValueError('PostgreSQL COPY table name must not be empty.');
        }
        if (in_array('', $parts, true)) {
            throw new \ValueError('PostgreSQL COPY table name must not contain an empty qualifier component.');
        }
        if (count($parts) > 2) {
            throw new \ValueError('PostgreSQL COPY table name may contain at most a schema and table component.');
        }

        $components = [];
        foreach ($parts as $part) {
            $tokens = SqlTokenStream::tokenize($part, PgSqlLexerProfile::create())->significantTokens();
            if (count($tokens) !== 1) {
                throw new \ValueError('PostgreSQL COPY table name must be an identifier or schema-qualified identifier.');
            }
            $components[] = $this->identifier($tokens[0], 'table name');
        }

        return $components;
    }

    /** @return list<string> */
    private function columns(?string $fields, TableDefinition $definition): array
    {
        if ($fields === null) {
            $columns = [];
            foreach ($definition->columns as $column) {
                if (!isset($definition->generatedExpressions[$column])) {
                    $columns[] = $column;
                }
            }

            return $columns;
        }

        $parts = SqlTokenStream::tokenize($fields, PgSqlLexerProfile::create())->splitTopLevel();
        if ($parts === [] || in_array('', $parts, true)) {
            throw new \ValueError('PostgreSQL COPY fields must contain at least one column identifier.');
        }

        $columns = [];
        foreach ($parts as $part) {
            $tokens = SqlTokenStream::tokenize($part, PgSqlLexerProfile::create())->significantTokens();
            if (count($tokens) !== 1) {
                throw new \ValueError('Each PostgreSQL COPY field must be a single column identifier.');
            }
            $column = $this->identifier($tokens[0], 'field');
            if (in_array($column, $columns, true)) {
                throw new \ValueError(sprintf('PostgreSQL COPY field "%s" is specified more than once.', $column));
            }
            $columns[] = $column;
        }

        return $columns;
    }

    /** @param non-empty-list<string> $columns */
    private function columnListSql(array $columns): string
    {
        return implode(', ', array_map($this->quoteIdentifier(...), $columns));
    }

    private function relationSql(CopyTarget $target): string
    {
        return implode('.', array_map($this->quoteIdentifier(...), $target->relation));
    }

    /** @param list<mixed> $values */
    public function encodeRow(array $values, string $separator, string $nullAs): string
    {
        $this->validateSeparator($separator);
        $encoded = [];
        foreach ($values as $value) {
            if ($value === null) {
                $encoded[] = $nullAs;
                continue;
            }

            $encoded[] = $this->escape($this->copyOutput($value), $separator);
        }

        return implode($separator, $encoded) . "\n";
    }

    /** @return list<string|null> */
    public function decodeRow(string $row, string $separator, string $nullAs): array
    {
        $this->validateSeparator($separator);
        if (str_ends_with($row, "\r\n")) {
            $row = substr($row, 0, -2);
        } elseif (str_ends_with($row, "\n") || str_ends_with($row, "\r")) {
            $row = substr($row, 0, -1);
        }
        if (str_contains($row, "\n") || str_contains($row, "\r")) {
            throw new \ValueError('PostgreSQL COPY rows must escape embedded newlines and carriage returns.');
        }
        if ($row === '\\.') {
            throw new \ValueError('PostgreSQL COPY end-of-data markers are not row values.');
        }

        return $this->decodeFields($row, $separator, $nullAs);
    }

    private function identifier(SqlToken $token, string $subject): string
    {
        $parsed = SqlTokenStream::tokenize($token->text, PgSqlLexerProfile::create())->identifierAt();
        if ($parsed === null) {
            throw new \ValueError(sprintf('PostgreSQL COPY %s must be a valid identifier.', $subject));
        }

        return $token->kind === SqlTokenKind::Word ? strtolower($parsed['name']) : $parsed['name'];
    }

    private function quoteIdentifier(string $identifier): string
    {
        return '"' . str_replace('"', '""', $identifier) . '"';
    }

    private function validateSeparator(string $separator): void
    {
        if (strlen($separator) !== 1) {
            throw new \ValueError('PostgreSQL COPY separator must be exactly one byte.');
        }
    }

    private function copyOutput(mixed $value): string
    {
        if (is_string($value) || is_int($value) || is_float($value)) {
            return (string) $value;
        }
        if (is_bool($value)) {
            return $value ? 't' : 'f';
        }
        if (is_resource($value)) {
            $bytes = stream_get_contents($value);
            if ($bytes === false) {
                throw new \ValueError('PostgreSQL COPY could not read a binary value.');
            }

            return '\\x' . bin2hex($bytes);
        }

        throw new \ValueError(sprintf('PostgreSQL COPY cannot encode a value of type %s.', get_debug_type($value)));
    }

    private function escape(string $value, string $separator): string
    {
        $result = '';
        $length = strlen($value);
        for ($index = 0; $index < $length; $index++) {
            $character = $value[$index];
            $escaped = match ($character) {
                "\x08" => '\\b',
                "\x0C" => '\\f',
                "\n" => '\\n',
                "\r" => '\\r',
                "\t" => '\\t',
                "\x0B" => '\\v',
                '\\' => '\\\\',
                default => null,
            };
            $result .= $escaped ?? ($character === $separator ? '\\' . $character : $character);
        }

        return $result;
    }

    /** @return list<string|null> */
    private function decodeFields(string $row, string $separator, string $nullAs): array
    {
        $values = [];
        $decoded = '';
        $fieldStart = 0;
        $length = strlen($row);
        for ($index = 0; $index <= $length; $index++) {
            if ($index === $length || $row[$index] === $separator) {
                $raw = substr($row, $fieldStart, $index - $fieldStart);
                $values[] = $raw === $nullAs ? null : $decoded;
                $decoded = '';
                $fieldStart = $index + 1;
                continue;
            }

            $character = $row[$index];
            if ($character !== '\\') {
                $decoded .= $character;
                continue;
            }

            $next = $row[$index + 1] ?? null;
            if ($next === null) {
                throw new \ValueError('PostgreSQL COPY field ends with an incomplete backslash escape.');
            }
            $index++;

            if ($next >= '0' && $next <= '7') {
                $digits = $next;
                while (strlen($digits) < 3) {
                    $following = $row[$index + 1] ?? null;
                    if ($following === null) {
                        break;
                    }
                    if ($following < '0') {
                        break;
                    }
                    if ($following > '7') {
                        break;
                    }
                    $index++;
                    $digit = $following;
                    $digits .= $digit;
                }
                $byte = intval($digits, 8);
                if ($byte < 0 || $byte > 255) {
                    throw new \ValueError('PostgreSQL COPY octal escape must fit in one byte.');
                }
                $decoded .= chr($byte);
                continue;
            }
            $following = $row[$index + 1] ?? null;
            if ($next === 'x' && $following !== null && ctype_xdigit($following)) {
                $index++;
                $digit = $following;
                $digits = $digit;
                $following = $row[$index + 1] ?? null;
                if ($following !== null && ctype_xdigit($following)) {
                    $index++;
                    $digit = $following;
                    $digits .= $digit;
                }
                $byte = intval($digits, 16);
                if ($byte < 0) {
                    throw new \ValueError('PostgreSQL COPY hexadecimal escape must not be negative.');
                }
                if ($byte > 255) {
                    throw new \ValueError('PostgreSQL COPY hexadecimal escape must fit in one byte.');
                }
                $decoded .= chr($byte);
                continue;
            }

            $decoded .= match ($next) {
                'b' => "\x08",
                'f' => "\x0C",
                'n' => "\n",
                'r' => "\r",
                't' => "\t",
                'v' => "\x0B",
                default => $next,
            };
        }

        return $values;
    }
}
