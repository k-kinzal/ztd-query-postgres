<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression;

use ZtdQuery\Exception\UnsupportedSqlException;
use ZtdQuery\Shadow\Mutation\UpsertColumnSource;
use ZtdQuery\Shadow\Mutation\UpsertExpression;
use ZtdQuery\Sql\SqlToken;
use ZtdQuery\Sql\SqlTokenKind;

/**
 * Primary parser for the supported UPSERT expression grammar.
 *
 * @visibility root
 */
final class PrimaryParser
{
    /**
     * Shares the token position with the enclosing expression parser.
     */
    public function __construct(private readonly ExpressionCursor $cursor)
    {
    }

    /**
     * Parse primary.
     * @throws UnsupportedSqlException
     */
    public function parsePrimary(): UpsertExpression
    {
        $token = $this->cursor->tokens[$this->cursor->index] ?? null;
        if ($token === null) {
            throw new UnsupportedSqlException($this->cursor->sql, 'Unsupported UPSERT expression');
        }
        if ((new ExpressionLexeme())->isSymbol($token, ['('])) {
            $this->cursor->index++;
            $expression = (new PrecedenceParser($this->cursor))->parseOr();
            if (!isset($this->cursor->tokens[$this->cursor->index]) || !(new ExpressionLexeme())->isSymbol($this->cursor->tokens[$this->cursor->index], [')'])) {
                throw new UnsupportedSqlException($this->cursor->sql, 'Unsupported UPSERT expression');
            }
            $this->cursor->index++;

            return $expression;
        }
        if ($token->kind === SqlTokenKind::Number) {
            $this->cursor->index++;

            return UpsertExpression::literal((new ExpressionLexeme())->number($token->text));
        }
        if ($token->kind === SqlTokenKind::String) {
            $this->cursor->index++;

            return UpsertExpression::literal((new ExpressionLexeme())->string($token->text));
        }
        if ($token->isKeyword('NULL')) {
            $this->cursor->index++;

            return UpsertExpression::literal(null);
        }
        if ($token->isKeyword('TRUE') || $token->isKeyword('FALSE')) {
            $this->cursor->index++;

            return UpsertExpression::literal($token->isKeyword('TRUE'));
        }
        return $this->column($token);
    }

    /**
     * Column source.
     * @throws UnsupportedSqlException
     */
    public function columnSource(string $qualifier): UpsertColumnSource
    {
        if (strcasecmp($qualifier, 'EXCLUDED') === 0) {
            return UpsertColumnSource::Incoming;
        }
        if (strcasecmp($qualifier, $this->cursor->tableName) === 0) {
            return UpsertColumnSource::Existing;
        }

        throw new UnsupportedSqlException($this->cursor->sql, 'Unsupported UPSERT expression');
    }
    /**
     * Reads a bare column or a qualified existing/incoming column reference.
     * @throws UnsupportedSqlException
     */
    public function column(SqlToken $token): UpsertExpression
    {
        if (!(new ExpressionLexeme())->isIdentifier($token)) {
            throw new UnsupportedSqlException($this->cursor->sql, 'Unsupported UPSERT expression');
        }

        $identifier = (new ExpressionLexeme())->identifier($token);
        $this->cursor->index++;
        if (isset($this->cursor->tokens[$this->cursor->index]) && (new ExpressionLexeme())->isSymbol($this->cursor->tokens[$this->cursor->index], ['.'])) {
            $this->cursor->index++;
            $column = $this->cursor->tokens[$this->cursor->index] ?? null;
            if ($column === null || !(new ExpressionLexeme())->isIdentifier($column)) {
                throw new UnsupportedSqlException($this->cursor->sql, 'Unsupported UPSERT expression');
            }
            $this->cursor->index++;

            return UpsertExpression::column(
                $this->columnSource($identifier),
                (new ExpressionLexeme())->identifier($column),
            );
        }

        return UpsertExpression::column(UpsertColumnSource::Existing, $identifier);
    }
}
