<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Sql\Transaction;

use ZtdQuery\Sql\SqlToken;
use ZtdQuery\Sql\SqlTokenKind;

/**
 * Keyword form operations for PostgreSQL transaction.
 *
 * @visibility root
 */
final class KeywordForm
{
    /**
     * @param list<SqlToken> $tokens
     * @param list<list<string>> $forms
     */
    public function matchesAny(array $tokens, array $forms): bool
    {
        foreach ($forms as $form) {
            if ($this->matches($tokens, $form)) {
                return true;
            }
        }

        return false;
    }

    /**
     * @param list<SqlToken> $tokens
     * @param list<list<string>> $prefixes
     */
    public function nameAfter(array $tokens, array $prefixes): ?string
    {
        foreach ($prefixes as $prefix) {
            if (count($tokens) !== count($prefix) + 1 || !$this->matches(array_slice($tokens, 0, -1), $prefix)) {
                continue;
            }
            $name = $tokens[count($prefix)];
            if (!in_array($name->kind, [SqlTokenKind::Word, SqlTokenKind::QuotedIdentifier], true)) {
                return null;
            }

            return $this->unquote($name->text);
        }

        return null;
    }

    /**
     * @param list<SqlToken> $tokens
     * @param list<string> $keywords
     */
    public function matches(array $tokens, array $keywords): bool
    {
        if (count($tokens) !== count($keywords)) {
            return false;
        }
        foreach ($keywords as $index => $keyword) {
            if (!$tokens[$index]->isKeyword($keyword)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Unquote.
     */
    public function unquote(string $identifier): ?string
    {
        $first = $identifier[0] ?? '';
        if ($first === '`') {
            return null;
        }
        if ($first !== '"') {
            return $identifier;
        }
        if (($identifier[strlen($identifier) - 1] ?? '') !== '"') {
            return null;
        }

        return str_replace('""', '"', substr($identifier, 1, -1));
    }
}
