<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression;

use ZtdQuery\Sql\SqlToken;
use ZtdQuery\Sql\SqlTokenKind;

/**
 * Expression lexeme for the supported UPSERT expression grammar.
 *
 * @visibility root
 */
final class ExpressionLexeme
{
    /**
     * @param list<string> $symbols
     */
    public function isSymbol(SqlToken $token, array $symbols): bool
    {
        return $token->kind === SqlTokenKind::Symbol && in_array($token->text, $symbols, true);
    }

    /**
     * Is identifier.
     */
    public function isIdentifier(SqlToken $token): bool
    {
        if ($token->kind === SqlTokenKind::Word) {
            return true;
        }

        return $token->kind === SqlTokenKind::QuotedIdentifier && str_starts_with($token->text, '"');
    }

    /**
     * Identifier.
     */
    public function identifier(SqlToken $token): string
    {
        if ($token->kind === SqlTokenKind::Word) {
            return $token->text;
        }

        return str_replace('""', '"', substr($token->text, 1, -1));
    }

    /**
     * Number.
     */
    public function number(string $literal): int|float
    {
        $literal = str_replace('_', '', $literal);

        return strpbrk($literal, '.eE') === false ? (int) $literal : (float) $literal;
    }

    /**
     * String.
     */
    public function string(string $literal): string
    {
        return str_replace("''", "'", substr($literal, 1, -1));
    }
}
