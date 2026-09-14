<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Connection\Copy;

use ValueError;
use ZtdQuery\Platform\CopySupport;
use ZtdQuery\Platform\CopyTarget;
use ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile;
use ZtdQuery\Schema\TableDefinition;
use ZtdQuery\Sql\SqlTokenStream;

/**
 * Copy support for PostgreSQL queries.
 */
final class PgSqlCopySupport implements CopySupport
{
    /**
     * Resolves the unqualified table name selected by the COPY arguments.
     */
    public function tableName(string $relation): string
    {
        $parts = (new TargetColumns())->relationParts($relation);

        return $parts[count($parts) - 1];
    }

    /**
     * Resolves COPY relation and column arguments against the reflected schema.
     * @throws ValueError
     */
    public function target(string $relation, ?string $fields, TableDefinition $definition): CopyTarget
    {
        $columns = (new TargetColumns())->columns($fields, $definition);
        if ($columns === []) {
            throw new ValueError('PostgreSQL COPY requires at least one non-generated column.');
        }

        return new CopyTarget((new TargetColumns())->relationParts($relation), $columns);
    }

    /**
     * Builds the SELECT statement that supplies COPY output rows.
     */
    public function selectSql(CopyTarget $target): string
    {
        return sprintf(
            'SELECT %s FROM %s',
            (new TargetSql())->columnListSql($target->columns),
            (new TargetSql())->relationSql($target),
        );
    }

    /**
     * Builds the INSERT statement for one decoded COPY input row.
     * @throws ValueError
     */
    public function insertSql(CopyTarget $target, int $rowCount, bool $overrideSystemValue): string
    {
        if ($rowCount < 1) {
            throw new ValueError('PostgreSQL COPY INSERT requires at least one row.');
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
            (new TargetSql())->relationSql($target),
            (new TargetSql())->columnListSql($target->columns),
            $overrideSystemValue ? ' OVERRIDING SYSTEM VALUE' : '',
            implode(', ', $valueRows),
        );
    }

    /**
     * Recognizes PostgreSQL COPY statements.
     */
    public function isCopyStatement(string $sql): bool
    {
        return SqlTokenStream::tokenize($sql, PgSqlLexerProfile::create())->firstTopLevelKeyword() === 'COPY';
    }

    /**
     * @param list<mixed> $values
     * @throws ValueError
     */
    public function encodeRow(array $values, string $separator, string $nullAs): string
    {
        (new TextFields())->validateSeparator($separator);
        $encoded = [];
        foreach ($values as $value) {
            if ($value === null) {
                $encoded[] = $nullAs;
                continue;
            }

            if (is_string($value) || is_int($value) || is_float($value)) {
                $output = (string) $value;
            } elseif (is_bool($value)) {
                $output = $value ? 't' : 'f';
            } elseif (is_resource($value)) {
                $bytes = stream_get_contents($value);
                if ($bytes === false) {
                    throw new ValueError('PostgreSQL COPY could not read a binary value.');
                }
                $output = '\\x' . bin2hex($bytes);
            } else {
                throw new ValueError(sprintf('PostgreSQL COPY cannot encode a value of type %s.', get_debug_type($value)));
            }
            $encoded[] = (new TextFields())->escape($output, $separator);
        }

        return implode($separator, $encoded) . "\n";
    }

    /**
     * @return list<string|null>
     * @throws ValueError
     */
    public function decodeRow(string $row, string $separator, string $nullAs): array
    {
        (new TextFields())->validateSeparator($separator);
        if (str_ends_with($row, "\r\n")) {
            $row = substr($row, 0, -2);
        } elseif (str_ends_with($row, "\n") || str_ends_with($row, "\r")) {
            $row = substr($row, 0, -1);
        }
        if (str_contains($row, "\n") || str_contains($row, "\r")) {
            throw new ValueError('PostgreSQL COPY rows must escape embedded newlines and carriage returns.');
        }
        if ($row === '\\.') {
            throw new ValueError('PostgreSQL COPY end-of-data markers are not row values.');
        }

        return (new TextFields())->decodeFields($row, $separator, $nullAs);
    }
}
