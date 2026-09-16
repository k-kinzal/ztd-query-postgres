<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Connection\Parameter;

/**
 * Tracks the source position and operand context while escaping PDO placeholders.
 *
 * @visibility root
 */
final class EscapeCursor
{
    /**
     * Next byte to inspect in the original SQL.
     */
    public int $position = 0;
    /**
     * Escaped SQL accumulated so far.
     */
    public string $result = '';
    /**
     * Whether a question mark at the current position is a placeholder.
     */
    public bool $expectsOperand = true;

    /**
     * Retains the source SQL while escaping its operator tokens.
     */
    public function __construct(public readonly string $sql)
    {
    }

    /**
     * Preserves the next source span and advances the scanner.
     */
    public function preserve(int $length): void
    {
        $this->result .= substr($this->sql, $this->position, $length);
        $this->position += $length;
    }
}
