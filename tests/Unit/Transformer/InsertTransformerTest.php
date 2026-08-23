<?php

declare(strict_types=1);

namespace Tests\Unit\Transformer;

use PHPUnit\Framework\TestCase;
use ZtdQuery\Exception\UnsupportedSqlException;
use ZtdQuery\Platform\CastRenderer;
use ZtdQuery\Platform\Postgres\PgSqlParser;
use ZtdQuery\Platform\Postgres\Transformer\InsertSelectRenderer;
use ZtdQuery\Platform\Postgres\Transformer\InsertTransformer;
use ZtdQuery\Platform\Postgres\Transformer\SelectTransformer;
use ZtdQuery\Schema\ColumnType;
use ZtdQuery\Schema\ColumnTypeFamily;
use ZtdQuery\Schema\IdentityGenerationStrategy;
use ZtdQuery\Schema\PartialUniqueIndex;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;
use ZtdQuery\Platform\Postgres\PgSqlCastRenderer;
use ZtdQuery\Platform\Postgres\PgSqlIdentifierQuoter;

#[CoversClass(InsertTransformer::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlSelectRelationParser::class)]
#[UsesClass(PgSqlParser::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlConflictTarget::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PostgreSqlLexicalMasker::class)]
#[UsesClass(SelectTransformer::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlTableSampleParser::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlTableSampleRewriter::class)]
#[UsesClass(PgSqlCastRenderer::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlValueRenderer::class)]
#[UsesClass(PgSqlIdentifierQuoter::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Transformer\InsertRowRenderer::class)]
#[UsesClass(InsertSelectRenderer::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlCteShadowComposer::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlNativeUpsertProjector::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlGeneratedColumnProjector::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlLexerProfile::class)]
final class InsertTransformerTest extends TestCase
{
    public function testProjectsConflictExpressionUsingCandidateKeys(): void
    {
        $transformer = new InsertTransformer(new PgSqlParser(), new SelectTransformer());
        $tables = [
            'users' => [
                'rows' => [],
                'columns' => ['id', 'name'],
                'columnTypes' => [],
                'candidateKeys' => ['PRIMARY' => ['id']],
            ],
        ];

        $result = $transformer->transform(
            "INSERT INTO users (id, name) VALUES (1, 'Alice') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name",
            $tables,
        );

        self::assertStringContainsString('"__ztd_incoming"."name"', $result);
        self::assertStringContainsString('__ztd_upsert_value_0', $result);
        self::assertStringNotContainsString('EXCLUDED.', $result);
    }

    public function testProjectsPartialIndexPredicateForBothCandidateRows(): void
    {
        $transformer = new InsertTransformer(new PgSqlParser(), new SelectTransformer());
        $partial = new PartialUniqueIndex('users_active_email', ['email'], "status = 'active'::text");
        $tables = [
            'users' => [
                'rows' => [],
                'columns' => ['email', 'status', 'login_count'],
                'columnTypes' => [],
                'candidateKeys' => [],
                'partialUniqueIndexes' => [$partial->name => $partial],
            ],
        ];

        $result = $transformer->transform(
            "INSERT INTO users VALUES ('alice@example.com', 'active', 1) "
            . "ON CONFLICT (email) WHERE status = 'active' "
            . 'DO UPDATE SET login_count = users.login_count + EXCLUDED.login_count',
            $tables,
        );

        self::assertStringContainsString(
            '("__ztd_existing"."status" = \'active\') AND ("__ztd_incoming"."status" = \'active\')',
            $result,
        );
        self::assertStringContainsString('__ztd_upsert_value_0', $result);
    }

    public function testUsesInjectedCastRendererForTypedValue(): void
    {
        $castRenderer = self::createStub(CastRenderer::class);
        $castRenderer->method('renderCast')->willReturn('CUSTOM_CAST');
        $transformer = new InsertTransformer(new PgSqlParser(), new SelectTransformer(), $castRenderer);
        $tables = [
            'users' => [
                'rows' => [],
                'columns' => ['id'],
                'columnTypes' => ['id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER')],
            ],
        ];

        self::assertStringContainsString(
            'SELECT CUSTOM_CAST AS "id"',
            $transformer->transform('INSERT INTO users (id) VALUES (1)', $tables),
        );
    }

    public function testBooleanPlaceholderUsesNullSafeCastOnlyForBooleanPlaceholder(): void
    {
        $transformer = new InsertTransformer(new PgSqlParser(), new SelectTransformer());
        $booleanTables = [
            'flags' => [
                'rows' => [],
                'columns' => ['enabled'],
                'columnTypes' => ['enabled' => new ColumnType(ColumnTypeFamily::BOOLEAN, 'BOOLEAN')],
            ],
        ];
        $integerTables = [
            'numbers' => [
                'rows' => [],
                'columns' => ['value'],
                'columnTypes' => ['value' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER')],
            ],
        ];

        self::assertStringContainsString(
            "CAST(COALESCE(NULLIF(CAST(? AS TEXT), ''), 'false') AS BOOLEAN)",
            $transformer->transform('INSERT INTO flags (enabled) VALUES (?)', $booleanTables),
        );
        self::assertStringContainsString(
            'CAST(TRUE AS BOOLEAN)',
            $transformer->transform('INSERT INTO flags (enabled) VALUES (TRUE)', $booleanTables),
        );
        self::assertStringContainsString(
            'CAST(? AS INTEGER)',
            $transformer->transform('INSERT INTO numbers (value) VALUES (?)', $integerTables),
        );
    }

    public function testInsertValuesWithExplicitColumns(): void
    {
        $transformer = new InsertTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = "INSERT INTO users (id, name) VALUES (1, 'Alice')";
        $tables = [
            'users' => [
                'rows' => [],
                'columns' => ['id', 'name'],
                'columnTypes' => [
                    'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                    'name' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
                ],
            ],
        ];

        $result = $transformer->transform($sql, $tables);
        self::assertStringContainsString('SELECT', $result);
        self::assertStringContainsString('"id"', $result);
        self::assertStringContainsString('"name"', $result);
    }

    public function testInsertValuesWithoutColumnsUsesTableContext(): void
    {
        $transformer = new InsertTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = "INSERT INTO users VALUES (1, 'Bob')";
        $tables = [
            'users' => [
                'rows' => [],
                'columns' => ['id', 'name'],
                'columnTypes' => [
                    'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                    'name' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
                ],
            ],
        ];

        $result = $transformer->transform($sql, $tables);
        self::assertStringContainsString('SELECT', $result);
        self::assertStringContainsString('"id"', $result);
        self::assertStringContainsString('"name"', $result);
    }

    public function testInsertMultipleRows(): void
    {
        $transformer = new InsertTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = "INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')";
        $tables = [
            'users' => [
                'rows' => [],
                'columns' => ['id', 'name'],
                'columnTypes' => [
                    'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                    'name' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
                ],
            ],
        ];

        $result = $transformer->transform($sql, $tables);
        self::assertStringContainsString('SELECT', $result);
        self::assertStringContainsString('UNION ALL', $result);
    }

    public function testInsertSelect(): void
    {
        $transformer = new InsertTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = 'INSERT INTO archive (id, name) SELECT id, name FROM users WHERE active = false';
        $tables = [
            'users' => [
                'rows' => [['id' => 1, 'name' => 'Alice']],
                'columns' => ['id', 'name'],
                'columnTypes' => [
                    'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                    'name' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
                ],
            ],
        ];

        $result = $transformer->transform($sql, $tables);
        self::assertStringContainsString('SELECT', $result);
        self::assertStringContainsString('"users"', $result);
        self::assertStringContainsString('AS MATERIALIZED', $result);
    }

    public function testInsertSelectUsesExplicitTargetColumnsForProjectionAndIdentity(): void
    {
        $transformer = new InsertTransformer(new PgSqlParser(), new SelectTransformer());
        $tables = ['archive' => [
            'rows' => [],
            'columns' => ['id', 'name', 'status'],
            'columnTypes' => [],
            'identityStrategies' => ['id' => IdentityGenerationStrategy::Sequence],
        ]];

        $result = $transformer->transform('INSERT INTO archive (name) SELECT name FROM users', $tables);

        self::assertStringContainsString('1 + ROW_NUMBER() OVER () - 1 AS "id"', $result);
        self::assertStringContainsString('"__ztd_insert_0" AS "name"', $result);
        self::assertStringContainsString('NULL AS "status"', $result);
    }

    public function testInsertWithoutTableThrowsException(): void
    {
        $transformer = new InsertTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = 'INSERT INTO';
        $this->expectException(UnsupportedSqlException::class);
        $transformer->transform($sql, []);
    }

    public function testInsertWithoutColumnsAndNoTableContextThrowsException(): void
    {
        $transformer = new InsertTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = "INSERT INTO unknown_table VALUES (1, 'test')";
        $this->expectException(UnsupportedSqlException::class);
        $this->expectExceptionMessage('Cannot determine columns');
        $transformer->transform($sql, []);
    }

    public function testInsertValueCountMismatchThrowsException(): void
    {
        $transformer = new InsertTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = "INSERT INTO users (id, name, email) VALUES (1, 'Alice')";
        $tables = [
            'users' => [
                'rows' => [],
                'columns' => ['id', 'name', 'email'],
                'columnTypes' => [],
            ],
        ];

        $this->expectException(UnsupportedSqlException::class);
        $this->expectExceptionMessage('values count does not match');
        $transformer->transform($sql, $tables);
    }

    public function testInsertAppliesCteShadowing(): void
    {
        $transformer = new InsertTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = "INSERT INTO users (id, name) VALUES (2, 'Bob')";
        $tables = [
            'users' => [
                'rows' => [['id' => 1, 'name' => 'Alice']],
                'columns' => ['id', 'name'],
                'columnTypes' => [
                    'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                    'name' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
                ],
            ],
        ];

        $result = $transformer->transform($sql, $tables);
        self::assertStringContainsString('SELECT', $result);
    }

    public function testInsertWithSchemaQualifiedTable(): void
    {
        $transformer = new InsertTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = 'INSERT INTO public.users (id, name) VALUES (1, \'Test\')';
        $tables = [];

        $result = $transformer->transform($sql, $tables);
        self::assertStringContainsString('SELECT', $result);
    }

    public function testInsertValuesWithWhitespaceAroundValues(): void
    {
        $transformer = new InsertTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = "INSERT INTO users (id, name) VALUES ( 1 , 'Alice' )";
        $tables = [
            'users' => [
                'rows' => [],
                'columns' => ['id', 'name'],
                'columnTypes' => [
                    'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                    'name' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
                ],
            ],
        ];

        $result = $transformer->transform($sql, $tables);
        self::assertStringContainsString('CAST(1 AS INTEGER) AS "id"', $result);
        self::assertStringNotContainsString(' 1  AS', $result);
    }

    public function testInsertExactOutputFormat(): void
    {
        $transformer = new InsertTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = "INSERT INTO users (id, name) VALUES (1, 'Alice')";
        $tables = [];

        $result = $transformer->transform($sql, $tables);
        self::assertStringContainsString('SELECT 1 AS "id"', $result);
    }

    public function testInsertMultiRowExactFormat(): void
    {
        $transformer = new InsertTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = "INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')";
        $tables = [];

        $result = $transformer->transform($sql, $tables);
        self::assertSame(
            "SELECT 1 AS \"id\", 'Alice' AS \"name\" UNION ALL SELECT 2 AS \"id\", 'Bob' AS \"name\"",
            $result
        );
    }

    public function testInsertValuesColumnExprAppearsInOutput(): void
    {
        $transformer = new InsertTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = "INSERT INTO users (id, name) VALUES (1, 'Alice')";
        $tables = [];

        $result = $transformer->transform($sql, $tables);
        self::assertStringContainsString("'Alice' AS \"name\"", $result);
        self::assertStringContainsString('1 AS "id"', $result);
    }

    public function testInsertNoValuesThrows(): void
    {
        $transformer = new InsertTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = 'INSERT INTO users (id)';
        $tables = [];

        $this->expectException(UnsupportedSqlException::class);
        $transformer->transform($sql, $tables);
    }

    public function testInsertProjectsOmittedAndExplicitDefaults(): void
    {
        $transformer = new InsertTransformer(new PgSqlParser(), new SelectTransformer());
        $tables = ['users' => [
            'rows' => [],
            'columns' => ['id', 'status', 'note'],
            'columnTypes' => [],
            'columnDefaults' => ['status' => "'active'"],
        ]];

        $result = $transformer->transform('INSERT INTO users (id, status) VALUES (1, DEFAULT)', $tables);

        self::assertStringContainsString('1 AS "id"', $result);
        self::assertStringContainsString("'active' AS \"status\"", $result);
        self::assertStringContainsString('NULL AS "note"', $result);
    }

    public function testInsertDefaultValuesProjectsCompleteRow(): void
    {
        $transformer = new InsertTransformer(new PgSqlParser(), new SelectTransformer());
        $tables = ['settings' => [
            'rows' => [],
            'columns' => ['enabled', 'label'],
            'columnTypes' => [],
            'columnDefaults' => ['enabled' => 'TRUE', 'label' => "'new'"],
        ]];

        $result = $transformer->transform('INSERT INTO settings DEFAULT VALUES', $tables);

        self::assertStringContainsString('TRUE AS "enabled"', $result);
        self::assertStringContainsString("'new' AS \"label\"", $result);
    }

    public function testInsertNormalizesSparseTableColumnKeys(): void
    {
        $transformer = new InsertTransformer(new PgSqlParser(), new SelectTransformer());
        $tables = ['users' => [
            'rows' => [],
            'columns' => [2 => 'id', 5 => 'name'],
            'columnTypes' => [],
        ]];

        $result = $transformer->transform("INSERT INTO users (id, name) VALUES (1, 'Alice')", $tables);

        self::assertStringContainsString('1 AS "id"', $result);
        self::assertStringContainsString("'Alice' AS \"name\"", $result);
    }

    public function testInsertAllocatesSerialValuesWithoutPhysicalSequence(): void
    {
        $transformer = new InsertTransformer(new PgSqlParser(), new SelectTransformer());
        $tables = ['users' => [
            'rows' => [],
            'columns' => ['id', 'name'],
            'columnTypes' => [],
            'identityStrategies' => ['id' => IdentityGenerationStrategy::Sequence],
        ]];

        $first = $transformer->transform("INSERT INTO users (name) VALUES ('Alice'), ('Bob')", $tables);
        $transformer->commitRewriteState();
        $second = $transformer->transform("INSERT INTO users (name) VALUES ('Carol')", $tables);

        self::assertStringContainsString('1 AS "id"', $first);
        self::assertStringContainsString('2 AS "id"', $first);
        self::assertStringContainsString('3 AS "id"', $second);
    }

    public function testParentAndPartitionShareSerialAllocationNamespace(): void
    {
        $transformer = new InsertTransformer(new PgSqlParser(), new SelectTransformer());
        $table = [
            'rows' => [],
            'columns' => ['id', 'name'],
            'columnTypes' => [],
            'identityStrategies' => ['id' => IdentityGenerationStrategy::Sequence],
        ];
        $tables = [
            'logs' => $table,
            'logs_2024' => $table + ['storageTable' => 'logs'],
        ];

        $parent = $transformer->transform("INSERT INTO logs (name) VALUES ('parent')", $tables);
        $transformer->commitRewriteState();
        $child = $transformer->transform("INSERT INTO logs_2024 (name) VALUES ('child')", $tables);

        self::assertStringContainsString('1 AS "id"', $parent);
        self::assertStringContainsString('2 AS "id"', $child);
    }

    public function testUncommittedTransformDoesNotConsumeSerialValue(): void
    {
        $transformer = new InsertTransformer(new PgSqlParser(), new SelectTransformer());
        $tables = ['users' => [
            'rows' => [],
            'columns' => ['id', 'name'],
            'columnTypes' => [],
            'identityStrategies' => ['id' => IdentityGenerationStrategy::Sequence],
        ]];

        $preview = $transformer->transform("INSERT INTO users (name) VALUES ('preview')", $tables);
        $executed = $transformer->transform("INSERT INTO users (name) VALUES ('executed')", $tables);

        self::assertStringContainsString('1 AS "id"', $preview);
        self::assertStringContainsString('1 AS "id"', $executed);
    }

    public function testInsertAllocatesSerialValueAfterExistingRows(): void
    {
        $transformer = new InsertTransformer(new PgSqlParser(), new SelectTransformer());
        $tables = ['users' => [
            'rows' => [['id' => 7, 'name' => 'Existing']],
            'columns' => ['id', 'name'],
            'columnTypes' => [],
            'identityStrategies' => ['id' => IdentityGenerationStrategy::MaxValue],
        ]];

        $result = $transformer->transform("INSERT INTO users (name) VALUES ('Alice')", $tables);

        self::assertStringContainsString('8 AS "id"', $result);
    }

    public function testExplicitIdentityDoesNotConsumeGeneratedIdentity(): void
    {
        $transformer = new InsertTransformer(new PgSqlParser(), new SelectTransformer());
        $tables = ['users' => [
            'rows' => [],
            'columns' => ['id', 'name'],
            'columnTypes' => [],
            'identityStrategies' => ['id' => IdentityGenerationStrategy::Sequence],
        ]];

        $transformer->transform("INSERT INTO users (id, name) VALUES (42, 'explicit')", $tables);
        $transformer->commitRewriteState();
        $generated = $transformer->transform("INSERT INTO users (name) VALUES ('generated')", $tables);

        self::assertStringContainsString('1 AS "id"', $generated);
    }
}
