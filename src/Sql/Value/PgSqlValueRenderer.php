<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Sql\Value;

use RuntimeException;
use Stringable;
use ZtdQuery\Platform\CastRenderer;
use ZtdQuery\Platform\ValueRenderer;
use ZtdQuery\Schema\ColumnDeclaration;
use ZtdQuery\Schema\ColumnTypeFamily;

/**
 * Encodes shadow values using PostgreSQL's typed literal semantics.
 *
 * @visibility public
 * @example Encode boolean values
 *     (new \ZtdQuery\Platform\Postgres\Sql\Value\PgSqlValueRenderer())->renderValue(true) // => 'TRUE'
 */
final class PgSqlValueRenderer implements ValueRenderer
{
    /**
     * Initializes the collaborators and state used by this value renderer.
     */
    public function __construct(private readonly CastRenderer $castRenderer = new PgSqlCastRenderer())
    {
    }

    /**
     * Renders a shadow value as a PostgreSQL literal, optionally using its declared type.
     * @throws RuntimeException
     *
     * @visibility public
     * @example Render typed strings and null without interpolating data into SQL
     *     $renderer = new \ZtdQuery\Platform\Postgres\Sql\Value\PgSqlValueRenderer();
     *     $renderer->renderValue("O'Reilly") // => "CAST('O''Reilly' AS TEXT)"
     *     $renderer->renderValue(null) // => 'NULL'
     */
    public function renderValue(mixed $value, ?ColumnDeclaration $type = null): string
    {
        if ($value === null) {
            return 'NULL';
        }

        $text = new LiteralText();
        if ($type === null) {
            if (is_bool($value) || is_float($value) || $value instanceof Stringable) {
                return $text->untyped($value);
            }
            if (!is_int($value) && !is_string($value)) {
                throw new RuntimeException('Unsupported value type for CTE shadowing.');
            }
            $inferred = new ColumnDeclaration(is_int($value) ? ColumnTypeFamily::INTEGER : ColumnTypeFamily::TEXT, is_int($value) ? 'INTEGER' : 'TEXT');
            return $this->castRenderer->renderCast(is_int($value) ? (string) $value : $text->quoted($value), $inferred);
        }
        if ($type->family === ColumnTypeFamily::BINARY) {
            if (is_resource($value) && get_resource_type($value) === 'stream') {
                $bytes = (new BinaryStream())->read($value);
            } elseif (is_scalar($value) || $value instanceof Stringable) {
                $bytes = (string) $value;
            } else {
                throw new RuntimeException('Unsupported value type for CTE shadowing.');
            }
            $expression = "decode('" . bin2hex($bytes) . "', 'hex')";
        } else {
            $expression = $text->quoted(is_scalar($value) || $value instanceof Stringable ? $value : serialize($value));
        }
        return $this->castRenderer->renderCast($expression, $type);
    }
}
