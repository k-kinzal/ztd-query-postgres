<?php

declare(strict_types=1);

namespace Tests\Unit\Schema\Reflection\Column;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Schema\Reflection\Column\ColumnDefinitionSql::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlIdentifierQuoter::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Schema\Reflection\Column\NativeTypeSql::class)]
final class ColumnDefinitionSqlTest extends TestCase
{
    public function testBuildColumnDefinition(): void
    {
        self::assertSame('"id" INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY', (new \ZtdQuery\Platform\Postgres\Schema\Reflection\Column\ColumnDefinitionSql())->buildColumnDefinition(['column_name' => 'id', 'data_type' => 'integer', 'is_nullable' => 'NO', 'is_identity' => 'YES', 'identity_generation' => 'ALWAYS']));

        self::assertSame('"total" NUMERIC(10,2) GENERATED ALWAYS AS (price * quantity) STORED', (new \ZtdQuery\Platform\Postgres\Schema\Reflection\Column\ColumnDefinitionSql())->buildColumnDefinition(['column_name' => 'total', 'data_type' => 'numeric', 'numeric_precision' => 10, 'numeric_scale' => 2, 'is_generated' => 'ALWAYS', 'generation_expression' => 'price * quantity']));

        self::assertSame('"label" VARCHAR(32) DEFAULT \'guest\'::text', (new \ZtdQuery\Platform\Postgres\Schema\Reflection\Column\ColumnDefinitionSql())->buildColumnDefinition(['column_name' => 'label', 'data_type' => 'character varying', 'character_maximum_length' => 32, 'column_default' => "'guest'::text"]));
    }

    public function testDomainTypeSql(): void
    {
        self::assertSame('"app"."positive_int"', (new \ZtdQuery\Platform\Postgres\Schema\Reflection\Column\ColumnDefinitionSql())->domainTypeSql(['domain_schema' => 'app', 'domain_name' => 'positive_int']));

        self::assertSame(null, (new \ZtdQuery\Platform\Postgres\Schema\Reflection\Column\ColumnDefinitionSql())->domainTypeSql([]));
    }
    public function testGenerationClausePrefersStoredExpressionsOverIdentityOrDefaults(): void
    {
        $column = new \ZtdQuery\Platform\Postgres\Schema\Reflection\Column\ColumnDefinitionSql();
        self::assertSame(' GENERATED ALWAYS AS (a + b) STORED', $column->generationClause(['is_generated' => 'ALWAYS', 'generation_expression' => 'a + b', 'is_identity' => 'YES', 'column_default' => '5']));
        self::assertSame(' DEFAULT 5', $column->generationClause(['column_default' => '5']));
    }
}
