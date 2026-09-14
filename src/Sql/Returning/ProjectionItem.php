<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Sql\Returning;

use ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile;
use ZtdQuery\Sql\SqlToken;
use ZtdQuery\Sql\SqlTokenKind;
use ZtdQuery\Sql\SqlTokenStream;

/**
 * Projection item operations for PostgreSQL returning.
 *
 * @visibility root
 */
final class ProjectionItem
{
    /**
     * @return array{source: string|null, output: string|null}|null
     */
    public function parseItem(string $expression): ?array
    {
        $tokens = SqlTokenStream::tokenize($expression, PgSqlLexerProfile::create())->significantTokens();
        $alias = null;
        $asIndex = $this->asIndex($tokens);
        if ($asIndex !== null) {
            if ($asIndex + 2 !== count($tokens)) {
                return null;
            }
            $aliasToken = $tokens[$asIndex + 1] ?? null;
            if ($aliasToken === null) {
                return null;
            }
            $alias = $this->identifierName($aliasToken);
            if ($alias === null) {
                return null;
            }
            $tokens = array_slice($tokens, 0, $asIndex);
        }

        if (!$this->isIdentifierPath($tokens)) {
            return null;
        }
        $last = $tokens[count($tokens) - 1];
        if ($last->text === '*') {
            return $alias === null ? ['source' => null, 'output' => null] : null;
        }

        $source = $this->identifierName($last);
        if ($source === null) {
            return null;
        }

        return ['source' => $source, 'output' => $alias];
    }

    /**
     * @param list<SqlToken> $tokens
     */
    public function asIndex(array $tokens): ?int
    {
        foreach ($tokens as $index => $token) {
            if ($token->isKeyword('AS')) {
                return $index;
            }
        }

        return null;
    }

    /**
     * @param list<SqlToken> $tokens
     */
    public function isIdentifierPath(array $tokens): bool
    {
        $lastIndex = count($tokens) - 1;
        foreach ($tokens as $index => $token) {
            if ($index % 2 === 0) {
                if ($this->identifierName($token) !== null) {
                    continue;
                }

                return $index === $lastIndex
                    && $token->kind === SqlTokenKind::Symbol
                    && $token->text === '*';
            }
            if ($token->kind !== SqlTokenKind::Symbol || $token->text !== '.') {
                return false;
            }
        }

        return true;
    }

    /**
     * Identifier name.
     */
    public function identifierName(SqlToken $token): ?string
    {
        if ($token->kind === SqlTokenKind::Word) {
            return $token->text;
        }
        if ($token->kind !== SqlTokenKind::QuotedIdentifier || strlen($token->text) <= 2) {
            return null;
        }

        $identifier = $token->text;
        if ($identifier[0] !== '"' || $identifier[strlen($identifier) - 1] !== '"') {
            return null;
        }

        return str_replace('""', '"', substr($identifier, 1, -1));
    }
}
