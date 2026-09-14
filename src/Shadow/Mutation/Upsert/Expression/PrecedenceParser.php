<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression;

use ZtdQuery\Exception\UnsupportedSqlException;
use ZtdQuery\Shadow\Mutation\UpsertExpression;
use ZtdQuery\Shadow\Mutation\UpsertExpressionKind;

/**
 * Precedence parser for the supported UPSERT expression grammar.
 *
 * @visibility root
 */
final class PrecedenceParser
{
    /**
     * Shares the token position with the enclosing expression parser.
     */
    public function __construct(private readonly ExpressionCursor $cursor)
    {
    }

    /**
     * Parse or.
     * @throws UnsupportedSqlException
     */
    public function parseOr(): UpsertExpression
    {
        $left = $this->parseAnd();
        while (($this->cursor->tokens[$this->cursor->index] ?? null)?->isKeyword('OR') === true) {
            $this->cursor->index++;
            $left = UpsertExpression::binary(UpsertExpressionKind::Or, $left, $this->parseAnd());
        }

        return $left;
    }

    /**
     * Parse and.
     * @throws UnsupportedSqlException
     */
    public function parseAnd(): UpsertExpression
    {
        $left = $this->parseComparison();
        while (($this->cursor->tokens[$this->cursor->index] ?? null)?->isKeyword('AND') === true) {
            $this->cursor->index++;
            $left = UpsertExpression::binary(UpsertExpressionKind::And, $left, $this->parseComparison());
        }

        return $left;
    }

    /**
     * Parse comparison.
     * @throws UnsupportedSqlException
     */
    public function parseComparison(): UpsertExpression
    {
        $left = $this->parseAdditive();
        $operator = (new ComparisonOperator($this->cursor))->comparisonOperator();

        return $operator === null
            ? $left
            : UpsertExpression::binary($operator, $left, $this->parseAdditive());
    }

    /**
     * Parse additive.
     * @throws UnsupportedSqlException
     */
    public function parseAdditive(): UpsertExpression
    {
        $left = $this->parseMultiplicative();
        while (isset($this->cursor->tokens[$this->cursor->index]) && (new ExpressionLexeme())->isSymbol($this->cursor->tokens[$this->cursor->index], ['+', '-'])) {
            $operator = $this->cursor->tokens[$this->cursor->index]->text;
            $this->cursor->index++;
            $left = UpsertExpression::binary(
                $operator === '+' ? UpsertExpressionKind::Add : UpsertExpressionKind::Subtract,
                $left,
                $this->parseMultiplicative(),
            );
        }

        return $left;
    }

    /**
     * Parse multiplicative.
     * @throws UnsupportedSqlException
     */
    public function parseMultiplicative(): UpsertExpression
    {
        $left = $this->parseUnary();
        while (isset($this->cursor->tokens[$this->cursor->index])
            && (new ExpressionLexeme())->isSymbol($this->cursor->tokens[$this->cursor->index], ['*', '/', '%'])
        ) {
            $operator = $this->cursor->tokens[$this->cursor->index]->text;
            $this->cursor->index++;
            $kind = $operator === '*'
                ? UpsertExpressionKind::Multiply
                : ($operator === '/' ? UpsertExpressionKind::Divide : UpsertExpressionKind::Modulo);
            $left = UpsertExpression::binary($kind, $left, $this->parseUnary());
        }

        return $left;
    }

    /**
     * Parse unary.
     * @throws UnsupportedSqlException
     */
    public function parseUnary(): UpsertExpression
    {
        $token = $this->cursor->tokens[$this->cursor->index] ?? null;
        if ($token?->isKeyword('NOT') === true) {
            $this->cursor->index++;

            return UpsertExpression::unary(UpsertExpressionKind::Not, $this->parseUnary());
        }
        if ($token !== null && (new ExpressionLexeme())->isSymbol($token, ['+', '-'])) {
            $this->cursor->index++;

            return UpsertExpression::unary(
                $token->text === '+' ? UpsertExpressionKind::UnaryPlus : UpsertExpressionKind::UnaryMinus,
                $this->parseUnary(),
            );
        }

        return (new PrimaryParser($this->cursor))->parsePrimary();
    }
}
