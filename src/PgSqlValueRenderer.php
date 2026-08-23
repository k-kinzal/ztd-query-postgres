<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres;

use Stringable;
use ZtdQuery\Platform\CastRenderer;
use ZtdQuery\Platform\ValueRenderer;
use ZtdQuery\Schema\ColumnType;
use ZtdQuery\Schema\ColumnTypeFamily;

/**
 * Encodes shadow values using PostgreSQL's typed literal semantics.
 */
final class PgSqlValueRenderer implements ValueRenderer
{
    public function __construct(private readonly CastRenderer $castRenderer = new PgSqlCastRenderer())
    {
    }

    public function renderValue(mixed $value, ?ColumnType $type = null): string
    {
        if ($value === null) {
            return 'NULL';
        }

        if ($type === null && is_bool($value)) {
            return $value ? 'TRUE' : 'FALSE';
        }

        if ($type === null && is_float($value)) {
            return var_export($value, true);
        }

        if ($type === null && $value instanceof Stringable) {
            return (string) $value;
        }

        if ($type === null && !is_scalar($value)) {
            throw new \RuntimeException('Unsupported value type for CTE shadowing.');
        }

        $resolvedType = $type ?? $this->inferType($value);
        $expression = $this->renderExpression($value, $resolvedType, $type !== null);

        return $this->castRenderer->renderCast($expression, $resolvedType);
    }

    private function renderExpression(mixed $value, ColumnType $type, bool $typed): string
    {
        if ($type->family === ColumnTypeFamily::BINARY) {
            $hex = bin2hex($this->stringValue($value));

            return "decode('$hex', 'hex')";
        }

        if (is_bool($value)) {
            return $this->quoteValue($value ? '1' : '0');
        }

        if (is_int($value) && !$typed) {
            return (string) $value;
        }

        if (is_float($value)) {
            return $this->quoteValue(var_export($value, true));
        }

        if (!is_scalar($value) && !$value instanceof Stringable) {
            return $this->quoteValue(serialize($value));
        }

        return $this->quoteValue($this->stringValue($value));
    }

    private function inferType(mixed $value): ColumnType
    {
        if (is_int($value)) {
            return new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER');
        }

        return new ColumnType(ColumnTypeFamily::TEXT, 'TEXT');
    }

    private function stringValue(mixed $value): string
    {
        if ($value instanceof Stringable) {
            return (string) $value;
        }
        if (is_scalar($value)) {
            return (string) $value;
        }
        if (is_resource($value) && get_resource_type($value) === 'stream') {
            return $this->readStream($value);
        }

        throw new \RuntimeException('Unsupported value type for CTE shadowing.');
    }

    /** @param resource $stream */
    private function readStream($stream): string
    {
        $position = ftell($stream);
        rewind($stream);
        $contents = stream_get_contents($stream);
        if ($position !== false) {
            fseek($stream, $position);
        }

        return $contents === false ? '' : $contents;
    }

    private function quoteValue(string $value): string
    {
        return "'" . str_replace("'", "''", $value) . "'";
    }
}
