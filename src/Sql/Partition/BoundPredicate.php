<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Sql\Partition;

use ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile;
use ZtdQuery\Schema\Partition\TablePartitionKey;
use ZtdQuery\Sql\SqlToken;
use ZtdQuery\Sql\SqlTokenStream;

/**
 * Bound predicate operations for PostgreSQL partition.
 *
 * @visibility root
 */
final class BoundPredicate
{
    /**
     * @param list<SqlToken> $tokens
     */
    public function rangePredicate(string $sql, array $tokens, int $index, TablePartitionKey $key): ?string
    {
        $from = $tokens[$index] ?? null;
        if (!$from instanceof SqlToken || !$from->isKeyword('FROM')) {
            return null;
        }
        $lower = (new ClauseTokens())->parenthesizedValues($sql, $tokens, $index + 1);
        if ($lower === null) {
            return null;
        }

        $to = $tokens[$lower['next']] ?? null;
        if (!$to instanceof SqlToken || !$to->isKeyword('TO')) {
            return null;
        }
        $upper = (new ClauseTokens())->parenthesizedValues($sql, $tokens, $lower['next'] + 1);
        if ($upper === null) {
            return null;
        }

        $lowerPredicate = $this->rangeBoundary($key->expressions, $lower['values'], '>=', 'MINVALUE');
        $upperPredicate = $this->rangeBoundary($key->expressions, $upper['values'], '<', 'MAXVALUE');
        if ($lowerPredicate === false || $upperPredicate === false) {
            return null;
        }

        $predicates = array_filter(
            [$lowerPredicate, $upperPredicate],
            static fn (string|false|null $predicate): bool => is_string($predicate),
        );

        return $predicates === [] ? 'TRUE' : implode(' AND ', $predicates);
    }

    /**
     * @param list<SqlToken> $tokens
     */
    public function listPredicate(string $sql, array $tokens, int $index, TablePartitionKey $key): ?string
    {
        if (count($key->expressions) !== 1) {
            return null;
        }
        $in = $tokens[$index] ?? null;
        if (!$in instanceof SqlToken || !$in->isKeyword('IN')) {
            return null;
        }
        $values = (new ClauseTokens())->parenthesizedValues($sql, $tokens, $index + 1);
        if ($values === null) {
            return null;
        }

        $nonNull = [];
        $hasNull = false;
        foreach ($values['values'] as $value) {
            $valueTokens = SqlTokenStream::tokenize($value, PgSqlLexerProfile::create())->significantTokens();
            if (count($valueTokens) === 1 && $valueTokens[0]->isKeyword('NULL')) {
                $hasNull = true;
                continue;
            }
            $nonNull[] = $value;
        }
        $expression = '(' . $key->expressions[0] . ')';
        $predicate = $nonNull === [] ? null : $expression . ' IN (' . implode(', ', $nonNull) . ')';
        if ($hasNull) {
            $nullPredicate = "$expression IS NULL";
            $predicate = $predicate === null ? $nullPredicate : "($predicate OR $nullPredicate)";
        }

        return $predicate;
    }

    /**
     * @param non-empty-list<string> $expressions
     * @param list<string> $values
     */
    public function rangeBoundary(array $expressions, array $values, string $operator, string $unbounded): string|false|null
    {
        if (count($expressions) !== count($values)) {
            return false;
        }

        $special = [];
        foreach ($values as $value) {
            $special[] = in_array(strtoupper($value), ['MINVALUE', 'MAXVALUE'], true);
        }
        if (!in_array(false, $special, true)) {
            foreach ($values as $value) {
                if (strcasecmp($value, $unbounded) !== 0) {
                    return false;
                }
            }

            return null;
        }
        if (in_array(true, $special, true)) {
            return false;
        }

        if (count($expressions) === 1) {
            return '(' . $expressions[0] . ") $operator " . $values[0];
        }

        return 'ROW(' . implode(', ', $expressions) . ") $operator ROW(" . implode(', ', $values) . ')';
    }
}
