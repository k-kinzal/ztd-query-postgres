<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Sql\Cte;

use ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile;
use ZtdQuery\Sql\SqlToken;
use ZtdQuery\Sql\SqlTokenKind;
use ZtdQuery\Sql\SqlTokenStream;

/**
 * Header parser operations for PostgreSQL cte.
 *
 * @visibility root
 */
final class HeaderParser
{
    /**
     * Returns the statement body after its WITH declarations.
     */
    public function statementSql(string $sql): string
    {
        $offset = $this->parseHeader($sql)['statementOffset'];

        return $offset === null ? $sql : substr($sql, $offset);
    }

    /**
     * @return array{names: list<string>, statementOffset: int|null}
     */
    public function parseHeader(string $sql): array
    {
        $tokens = $this->topLevelTokens($sql);
        if (($tokens[0] ?? null)?->isKeyword('WITH') !== true) {
            return ['names' => [], 'statementOffset' => null];
        }

        $index = 1;
        if (($tokens[$index] ?? null)?->isKeyword('RECURSIVE') === true) {
            $index++;
        }

        $names = [];
        while (isset($tokens[$index])) {
            $name = $this->identifierName($tokens[$index]);
            if ($name === null) {
                break;
            }
            $index++;

            $index = $this->bodyStartIndex($tokens, $index);

            if (!$this->isSymbol($tokens[$index] ?? null, '(')
                || !$this->isSymbol($tokens[$index + 1] ?? null, ')')
            ) {
                return ['names' => $names, 'statementOffset' => null];
            }
            $names[] = strtolower($name);
            $index += 2;

            $separator = $tokens[$index] ?? null;
            if (!$this->isSymbol($separator, ',')) {
                break;
            }
            $index++;
        }

        $statement = $tokens[$index] ?? null;

        return [
            'names' => $names,
            'statementOffset' => $statement?->offset,
        ];
    }

    /**
     * @param list<SqlToken> $tokens
     */
    public function findAsIndex(array $tokens, int $start): ?int
    {
        for ($index = $start; isset($tokens[$index]); $index++) {
            $token = $tokens[$index];
            if ($token->isKeyword('AS')) {
                return $index;
            }
            if ($token->kind === SqlTokenKind::Word) {
                return null;
            }
        }

        return null;
    }

    /**
     * Is symbol.
     */
    public function isSymbol(?SqlToken $token, string $symbol): bool
    {
        return $token instanceof SqlToken
            && $token->kind === SqlTokenKind::Symbol
            && $token->text === $symbol;
    }

    /**
     * Identifier name.
     */
    public function identifierName(SqlToken $token): ?string
    {
        if ($token->kind === SqlTokenKind::Word) {
            return $token->text;
        }
        if ($token->kind !== SqlTokenKind::QuotedIdentifier || strlen($token->text) < 2) {
            return null;
        }

        $quote = $token->text[0];
        $inner = substr($token->text, 1, -1);

        return str_replace($quote . $quote, $quote, $inner);
    }
    /**
     * Retains top-level tokens while treating each CTE body as an opaque span.
     * @return list<SqlToken>
     */
    public function topLevelTokens(string $sql): array
    {
        $tokens = [];
        foreach (SqlTokenStream::tokenize($sql, PgSqlLexerProfile::create())->significantTokens() as $token) {
            if ($token->isTopLevel()) {
                $tokens[] = $token;
            }
        }
        return $tokens;
    }

    /**
     * Skips AS and the optional materialization modifiers.
     * @param list<SqlToken> $tokens
     */
    public function bodyStartIndex(array $tokens, int $index): int
    {
        $asIndex = $this->findAsIndex($tokens, $index);
        $index = ($asIndex ?? count($tokens)) + 1;

        if (($tokens[$index] ?? null)?->isKeyword('NOT') === true) {
            $index++;
        }
        if (($tokens[$index] ?? null)?->isKeyword('MATERIALIZED') === true) {
            $index++;
        }

        return $index;
    }
}
