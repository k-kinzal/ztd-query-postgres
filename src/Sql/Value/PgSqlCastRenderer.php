<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Sql\Value;

use ZtdQuery\Platform\CastRenderer;
use ZtdQuery\Schema\ColumnDeclaration;

/**
 * PostgreSQL CAST expression renderer.
 *
 * Maps ColumnDeclaration to PostgreSQL-specific CAST syntax.
 * Uses standard CAST() syntax (not :: shorthand) for maximum compatibility.
 */
final class PgSqlCastRenderer implements CastRenderer
{
    /**
     * {@inheritDoc}
     */
    public function renderCast(string $expression, ColumnDeclaration $type): string
    {
        $castType = (new CastTypeMapper())->map($type);

        return "CAST($expression AS $castType)";
    }

    /**
     * {@inheritDoc}
     */
    public function renderNullCast(ColumnDeclaration $type): string
    {
        $castType = (new CastTypeMapper())->map($type);

        return "CAST(NULL AS $castType)";
    }
}
