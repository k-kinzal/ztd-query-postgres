<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Rewrite\Upsert;

use ZtdQuery\Platform\IdentifierQuoter;
use ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile;
use ZtdQuery\Sql\SqlToken;
use ZtdQuery\Sql\SqlTokenKind;
use ZtdQuery\Sql\SqlTokenStream;

/**
 * Expression binder operations for PostgreSQL upsert.
 *
 * @visibility root
 */
final class ExpressionBinder
{
    private const INCOMING_ALIAS = '__ztd_incoming';
    private const EXISTING_ALIAS = '__ztd_existing';
    /**
     * @var non-empty-list<string>
     */
    private readonly array $incomingNamespaces;
    private readonly IdentifierQuoter $quoter;

    /**
     * Supplies the dependencies used by this ExpressionBinder.
     * @param non-empty-list<string> $incomingNamespaces
     */
    public function __construct(array $incomingNamespaces, IdentifierQuoter $quoter)
    {
        $this->incomingNamespaces = $incomingNamespaces;
        $this->quoter = $quoter;
    }

    /**
     * @param list<string> $tableColumns
     */
    public function bindExpression(
        string $expression,
        string $tableName,
        array $tableColumns,
        string $unqualifiedAlias = self::EXISTING_ALIAS,
    ): string {
        $tokens = SqlTokenStream::tokenize($expression, PgSqlLexerProfile::create())->significantTokens();
        $subqueryTokens = $this->subqueryTokenIndexes($tokens);
        $replacements = [];
        $columnNames = array_fill_keys(array_map('strtolower', $tableColumns), true);
        $incomingNamespaces = array_fill_keys(array_map('strtolower', $this->incomingNamespaces), true);

        foreach ($tokens as $index => $token) {
            if (!$this->isIdentifier($token)) {
                continue;
            }
            if ($subqueryTokens[$index] ?? false) {
                continue;
            }
            $name = $this->identifier($token);
            $next = $tokens[$index + 1] ?? null;
            $afterNext = $tokens[$index + 2] ?? null;
            if ($next?->text === '.' && $afterNext !== null && $this->isIdentifier($afterNext)) {
                $replacement = $this->qualifiedReplacement($token, $afterNext, $name, $tableName, $incomingNamespaces);
                if ($replacement !== null) {
                    $replacements[] = $replacement;
                }
                continue;
            }
            $previous = $tokens[$index - 1] ?? null;
            if ($previous?->text === '.' || $next?->text === '(' || !isset($columnNames[strtolower($name)])) {
                continue;
            }
            $replacements[] = [
                'offset' => $token->offset,
                'length' => strlen($token->text),
                'value' => (new ConflictPredicate($this->quoter))->qualified($unqualifiedAlias, $name),
            ];
        }

        return $this->applyReplacements($expression, $replacements);
    }

    /**
     * @param list<SqlToken> $tokens
     * @return array<int, true>
     */
    public function subqueryTokenIndexes(array $tokens): array
    {
        $indexes = [];
        foreach ($tokens as $start => $token) {
            if (!$token->isKeyword('SELECT') || $token->isTopLevel()) {
                continue;
            }
            for ($index = $start; isset($tokens[$index]); ++$index) {
                $candidate = $tokens[$index];
                if ($candidate->depth < $token->depth) {
                    break;
                }
                $indexes[$index] = true;
            }
        }

        return $indexes;
    }

    /**
     * Is identifier.
     */
    public function isIdentifier(SqlToken $token): bool
    {
        return in_array($token->kind, [SqlTokenKind::Word, SqlTokenKind::QuotedIdentifier], true);
    }

    /**
     * Identifier.
     */
    public function identifier(SqlToken $token): string
    {
        $identifier = SqlTokenStream::tokenize($token->text, PgSqlLexerProfile::create())->identifierAt();

        return $identifier === null ? $token->text : $identifier['name'];
    }
    /**
     * Binds only an incoming or target-table qualifier, leaving other namespaces untouched.
     * @param array<string, true> $incomingNamespaces
     * @return array{offset: int, length: int, value: string}|null
     */
    public function qualifiedReplacement(SqlToken $token, SqlToken $afterNext, string $name, string $tableName, array $incomingNamespaces): ?array
    {
        $namespace = strtolower($name);
        $alias = isset($incomingNamespaces[$namespace])
            ? self::INCOMING_ALIAS
            : (strcasecmp($name, $tableName) === 0 ? self::EXISTING_ALIAS : null);
        if ($alias !== null) {
            return [
                'offset' => $token->offset,
                'length' => $afterNext->endOffset() - $token->offset,
                'value' => (new ConflictPredicate($this->quoter))->qualified($alias, $this->identifier($afterNext)),
            ];
        }
        return null;
    }

    /**
     * Replaces bound identifiers from right to left to preserve earlier source offsets.
     * @param list<array{offset: int, length: int, value: string}> $replacements
     */
    public function applyReplacements(string $expression, array $replacements): string
    {
        foreach (array_reverse($replacements) as $replacement) {
            $expression = substr_replace(
                $expression,
                $replacement['value'],
                $replacement['offset'],
                $replacement['length'],
            );
        }

        return $expression;
    }
}
