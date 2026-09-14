<?php

declare(strict_types=1);

namespace Tests\Unit\Rewrite\Transformer\Cte;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Rewrite\Transformer\Cte\RowSourceRenderer::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Value\PgSqlCastRenderer::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\GeneratedColumn\PgSqlGeneratedColumnProjector::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlIdentifierQuoter::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Value\PgSqlValueRenderer::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Value\CastTypeMapper::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Value\NativeCastTarget::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Value\BinaryStream::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Value\LiteralText::class)]
final class RowSourceRendererTest extends TestCase
{
    public function testGenerateCte(): void
    {
        self::assertSame('"users" AS MATERIALIZED (SELECT CAST(1 AS INTEGER) AS "id", CAST(\'Ada\' AS TEXT) AS "name")', (new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\Cte\RowSourceRenderer(new \ZtdQuery\Platform\Postgres\Sql\Value\PgSqlCastRenderer(), new \ZtdQuery\Platform\Postgres\Rewrite\GeneratedColumn\PgSqlGeneratedColumnProjector(), new \ZtdQuery\Platform\Postgres\Sql\PgSqlIdentifierQuoter(), new \ZtdQuery\Platform\Postgres\Sql\Value\PgSqlValueRenderer()))->generateCte('users', [['id' => 1, 'name' => 'Ada']], ['id', 'name'], [], []));

        self::assertSame('"users" AS MATERIALIZED (SELECT CAST(NULL AS INTEGER) AS "id" WHERE FALSE)', (new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\Cte\RowSourceRenderer(new \ZtdQuery\Platform\Postgres\Sql\Value\PgSqlCastRenderer(), new \ZtdQuery\Platform\Postgres\Rewrite\GeneratedColumn\PgSqlGeneratedColumnProjector(), new \ZtdQuery\Platform\Postgres\Sql\PgSqlIdentifierQuoter(), new \ZtdQuery\Platform\Postgres\Sql\Value\PgSqlValueRenderer()))->generateCte('users', [], ['id'], ['id' => new \ZtdQuery\Schema\ColumnDeclaration(\ZtdQuery\Schema\ColumnTypeFamily::INTEGER, 'INTEGER')], []));
    }

    public function testGenerateMultiRowSource(): void
    {
        self::assertSame('
  SELECT * FROM (VALUES
    (CAST(1 AS INTEGER)),
    (CAST(2 AS INTEGER))
  ) AS t("id")
', (new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\Cte\RowSourceRenderer(new \ZtdQuery\Platform\Postgres\Sql\Value\PgSqlCastRenderer(), new \ZtdQuery\Platform\Postgres\Rewrite\GeneratedColumn\PgSqlGeneratedColumnProjector(), new \ZtdQuery\Platform\Postgres\Sql\PgSqlIdentifierQuoter(), new \ZtdQuery\Platform\Postgres\Sql\Value\PgSqlValueRenderer()))->generateMultiRowSource([['id' => 1], ['id' => 2]], ['id'], []));
    }

    public function testWrapCte(): void
    {
        self::assertSame('"users" AS MATERIALIZED (SELECT 1 AS id)', (new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\Cte\RowSourceRenderer(new \ZtdQuery\Platform\Postgres\Sql\Value\PgSqlCastRenderer(), new \ZtdQuery\Platform\Postgres\Rewrite\GeneratedColumn\PgSqlGeneratedColumnProjector(), new \ZtdQuery\Platform\Postgres\Sql\PgSqlIdentifierQuoter(), new \ZtdQuery\Platform\Postgres\Sql\Value\PgSqlValueRenderer()))->wrapCte('"users"', 'SELECT 1 AS id', ['id'], []));
    }

    public function testFormatValue(): void
    {
        self::assertSame('NULL', (new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\Cte\RowSourceRenderer(new \ZtdQuery\Platform\Postgres\Sql\Value\PgSqlCastRenderer(), new \ZtdQuery\Platform\Postgres\Rewrite\GeneratedColumn\PgSqlGeneratedColumnProjector(), new \ZtdQuery\Platform\Postgres\Sql\PgSqlIdentifierQuoter(), new \ZtdQuery\Platform\Postgres\Sql\Value\PgSqlValueRenderer()))->formatValue(null));

        self::assertSame('TRUE', (new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\Cte\RowSourceRenderer(new \ZtdQuery\Platform\Postgres\Sql\Value\PgSqlCastRenderer(), new \ZtdQuery\Platform\Postgres\Rewrite\GeneratedColumn\PgSqlGeneratedColumnProjector(), new \ZtdQuery\Platform\Postgres\Sql\PgSqlIdentifierQuoter(), new \ZtdQuery\Platform\Postgres\Sql\Value\PgSqlValueRenderer()))->formatValue(true));

        self::assertSame('CAST(\'O\'\'Reilly\' AS TEXT)', (new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\Cte\RowSourceRenderer(new \ZtdQuery\Platform\Postgres\Sql\Value\PgSqlCastRenderer(), new \ZtdQuery\Platform\Postgres\Rewrite\GeneratedColumn\PgSqlGeneratedColumnProjector(), new \ZtdQuery\Platform\Postgres\Sql\PgSqlIdentifierQuoter(), new \ZtdQuery\Platform\Postgres\Sql\Value\PgSqlValueRenderer()))->formatValue('O\'Reilly'));

        self::assertSame('CAST(42 AS INTEGER)', (new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\Cte\RowSourceRenderer(new \ZtdQuery\Platform\Postgres\Sql\Value\PgSqlCastRenderer(), new \ZtdQuery\Platform\Postgres\Rewrite\GeneratedColumn\PgSqlGeneratedColumnProjector(), new \ZtdQuery\Platform\Postgres\Sql\PgSqlIdentifierQuoter(), new \ZtdQuery\Platform\Postgres\Sql\Value\PgSqlValueRenderer()))->formatValue(42));
    }

    public function testRenderFallbackNullCast(): void
    {
        self::assertSame('CAST(NULL AS TEXT)', (new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\Cte\RowSourceRenderer(new \ZtdQuery\Platform\Postgres\Sql\Value\PgSqlCastRenderer(), new \ZtdQuery\Platform\Postgres\Rewrite\GeneratedColumn\PgSqlGeneratedColumnProjector(), new \ZtdQuery\Platform\Postgres\Sql\PgSqlIdentifierQuoter(), new \ZtdQuery\Platform\Postgres\Sql\Value\PgSqlValueRenderer()))->renderFallbackNullCast());
    }
    public function testDeclaredSourceRendersEmptyTypedAndSingleRowSources(): void
    {
        $renderer = new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\Cte\RowSourceRenderer(new \ZtdQuery\Platform\Postgres\Sql\Value\PgSqlCastRenderer(), new \ZtdQuery\Platform\Postgres\Rewrite\GeneratedColumn\PgSqlGeneratedColumnProjector(), new \ZtdQuery\Platform\Postgres\Sql\PgSqlIdentifierQuoter(), new \ZtdQuery\Platform\Postgres\Sql\Value\PgSqlValueRenderer());
        self::assertSame('SELECT CAST(NULL AS TEXT) AS "name" WHERE FALSE', $renderer->declaredSource([], ['name'], []));
        self::assertSame("SELECT CAST('Ada' AS TEXT) AS \"name\"", $renderer->declaredSource([['name' => 'Ada']], ['name'], []));
    }
}
