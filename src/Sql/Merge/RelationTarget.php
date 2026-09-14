<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Sql\Merge;

use ZtdQuery\Exception\UnsupportedSqlException;
use ZtdQuery\Sql\SqlToken;
use ZtdQuery\Sql\SqlTokenKind;

/**
 * Relation target operations for PostgreSQL merge.
 *
 * @visibility root
 */
final class RelationTarget
{
    /**
     * @param list<SqlToken> $tokens
     * @return array{name: string, sql: string, next: int, last: SqlToken}|null
     */
    public function relationAt(string $sql, array $tokens, int $index): ?array
    {
        $first = $tokens[$index] ?? null;
        $name = $first !== null ? $this->identifierName($first) : null;
        if ($first === null || $name === null) {
            return null;
        }

        $start = $first->offset;
        $end = $first->endOffset();
        $last = $first;
        $index++;
        while ($this->isSymbol($tokens[$index] ?? null, '.')) {
            $component = $tokens[$index + 1] ?? null;
            if ($component === null) {
                return null;
            }
            $componentName = $this->identifierName($component);
            if ($componentName === null) {
                return null;
            }
            $name = $componentName;
            $end = $component->endOffset();
            $last = $component;
            $index += 2;
        }

        return [
            'name' => $name,
            'sql' => substr($sql, $start, $end - $start),
            'next' => $index,
            'last' => $last,
        ];
    }

    /**
     * @param list<SqlToken> $tokens
     * @throws UnsupportedSqlException
     */
    public function targetAlias(
        string $sql,
        array $tokens,
        int $start,
        int $end,
        string $default,
    ): string {
        if ($start === $end) {
            return $default;
        }
        if ($tokens[$start]->isKeyword('AS')) {
            $start++;
        }
        if ($start + 1 !== $end) {
            throw new UnsupportedSqlException($sql, 'Malformed MERGE target alias');
        }
        $alias = $this->identifierName($tokens[$start]);
        if ($alias === null) {
            throw new UnsupportedSqlException($sql, 'Malformed MERGE target alias');
        }

        return $alias;
    }

    /**
     * Identifier name.
     */
    public function identifierName(SqlToken $token): ?string
    {
        if ($token->kind === SqlTokenKind::Word) {
            return strtolower($token->text);
        }
        if ($token->kind !== SqlTokenKind::QuotedIdentifier || strlen($token->text) <= 2) {
            return null;
        }

        return str_replace('""', '"', substr($token->text, 1, -1));
    }

    /**
     * Is symbol.
     */
    public function isSymbol(?SqlToken $token, string $symbol): bool
    {
        if ($token === null) {
            return false;
        }
        return $token->text === $symbol;
    }
}
