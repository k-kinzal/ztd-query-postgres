<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression;

use ZtdQuery\Exception\UnsupportedSqlException;
use ZtdQuery\Shadow\Mutation\UpsertExpressionKind;

/**
 * Comparison operator for the supported UPSERT expression grammar.
 *
 * @visibility root
 */
final class ComparisonOperator
{
    /**
     * Shares the token position with the enclosing expression parser.
     */
    public function __construct(private readonly ExpressionCursor $cursor)
    {
    }

    /**
     * Comparison operator.
     * @throws UnsupportedSqlException
     */
    public function comparisonOperator(): ?UpsertExpressionKind
    {
        $first = $this->cursor->tokens[$this->cursor->index] ?? null;
        if ($first === null || !(new ExpressionLexeme())->isSymbol($first, ['=', '!', '<', '>'])) {
            return null;
        }
        $operator = $first->text;
        $second = $this->cursor->tokens[$this->cursor->index + 1] ?? null;
        if ($second !== null && (new ExpressionLexeme())->isSymbol($second, ['=', '>']) && $operator !== '=') {
            $operator .= $second->text;
            $this->cursor->index++;
        }
        $this->cursor->index++;

        return match ($operator) {
            '=' => UpsertExpressionKind::Equal,
            '!=', '<>' => UpsertExpressionKind::NotEqual,
            '<' => UpsertExpressionKind::Less,
            '<=' => UpsertExpressionKind::LessOrEqual,
            '>' => UpsertExpressionKind::Greater,
            '>=' => UpsertExpressionKind::GreaterOrEqual,
            default => throw new UnsupportedSqlException($this->cursor->sql, 'Unsupported UPSERT expression'),
        };
    }
}
