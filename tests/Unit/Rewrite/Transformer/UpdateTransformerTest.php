<?php

declare(strict_types=1);

namespace Tests\Unit\Rewrite\Transformer;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Exception\UnsupportedSqlException;
use ZtdQuery\Platform\Postgres\Rewrite\Transformer\SelectTransformer;
use ZtdQuery\Platform\Postgres\Rewrite\Transformer\UpdateTransformer;
use ZtdQuery\Platform\Postgres\Sql\PgSqlIdentifierQuoter;
use ZtdQuery\Platform\Postgres\Sql\PgSqlParser;
use ZtdQuery\Platform\Postgres\Sql\Value\PgSqlCastRenderer;
use ZtdQuery\Schema\ColumnDeclaration;
use ZtdQuery\Schema\ColumnTypeFamily;

#[CoversClass(UpdateTransformer::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Relation\PgSqlSelectRelationParser::class)]
#[UsesClass(PgSqlParser::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\PostgreSqlLexicalMasker::class)]
#[UsesClass(SelectTransformer::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Sampling\PgSqlTableSampleParser::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Sampling\PgSqlTableSampleRewriter::class)]
#[UsesClass(PgSqlCastRenderer::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Value\PgSqlValueRenderer::class)]
#[UsesClass(PgSqlIdentifierQuoter::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Cte\PgSqlCteShadowComposer::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\GeneratedColumn\PgSqlGeneratedColumnProjector::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::class)]
#[CoversClass(\ZtdQuery\Platform\Postgres\Rewrite\Transformer\Update\ColumnProjection::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Conflict\ColumnSet::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Cte\HeaderParser::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Cte\IdentifierReferences::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Cte\PrefixMerge::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Cte\ShadowDependencies::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Relation\FromClause::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Relation\RelationReference::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Sampling\SampleClause::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Sampling\SampleTokens::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Statement\Classification::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Statement\ConflictClause::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Statement\Identifiers::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Statement\InsertSource::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Statement\TableDefinitionClauses::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Conflict\PgSqlConflictTarget::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Sampling\PgSqlTableSample::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Sampling\PgSqlTableSampleMethod::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Sampling\SampleProjection::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Sampling\TableColumns::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Value\CastTypeMapper::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Lexing\CommentSpan::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Lexing\QuotedSpan::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Value\NativeCastTarget::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Value\BinaryStream::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Value\LiteralText::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Transformer\Cte\RowSourceRenderer::class)]
final class UpdateTransformerTest extends TestCase
{
    public function testBuildProjectionSimpleUpdate(): void
    {
        $transformer = new UpdateTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = "UPDATE users SET name = 'Bob' WHERE id = 1";
        $result = $transformer->buildProjection($sql, 'users', ['id', 'name']);

        self::assertSame('users', $result['table']);
        self::assertStringContainsString("'Bob' AS \"name\"", $result['sql']);
        self::assertStringContainsString('"users"."id"', $result['sql']);
        self::assertStringContainsString('FROM "users"', $result['sql']);
        self::assertStringContainsString('WHERE id = 1', $result['sql']);
    }

    public function testBuildProjectionWithAlias(): void
    {
        $transformer = new UpdateTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = "UPDATE users u SET name = 'Bob' WHERE id = 1";
        $result = $transformer->buildProjection($sql, 'users', ['id', 'name']);

        self::assertSame('users', $result['table']);
        self::assertStringContainsString('AS "u"', $result['sql']);
        self::assertStringContainsString('"u"."id"', $result['sql']);
    }

    public function testBuildProjectionWithMultipleSetColumns(): void
    {
        $transformer = new UpdateTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = "UPDATE users SET name = 'Bob', email = 'bob@example.com' WHERE id = 1";
        $result = $transformer->buildProjection($sql, 'users', ['id', 'name', 'email']);

        self::assertStringContainsString("'Bob' AS \"name\"", $result['sql']);
        self::assertStringContainsString("'bob@example.com' AS \"email\"", $result['sql']);
        self::assertStringContainsString('"users"."id"', $result['sql']);
    }

    public function testBuildProjectionWithFromClause(): void
    {
        $transformer = new UpdateTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = 'UPDATE users SET name = orders.name FROM orders WHERE users.id = orders.user_id';
        $result = $transformer->buildProjection($sql, 'users', ['id', 'name']);

        self::assertStringContainsString('orders', $result['sql']);
        self::assertStringContainsString('WHERE users.id = orders.user_id', $result['sql']);
    }

    public function testBuildProjectionWithNoWhereClause(): void
    {
        $transformer = new UpdateTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = 'UPDATE users SET active = true';
        $result = $transformer->buildProjection($sql, 'users', ['id', 'active']);

        self::assertStringContainsString('SELECT', $result['sql']);
        self::assertStringContainsString('FROM "users"', $result['sql']);
        self::assertStringNotContainsString('WHERE', $result['sql']);
    }

    public function testBuildProjectionWithEmptyColumnsUsesWildcard(): void
    {
        $transformer = new UpdateTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = "UPDATE users SET name = 'Bob' WHERE id = 1";
        $result = $transformer->buildProjection($sql, 'users', []);

        self::assertStringContainsString("'Bob' AS \"name\"", $result['sql']);
    }

    public function testBuildProjectionReturnsTablesArray(): void
    {
        $transformer = new UpdateTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = "UPDATE users SET name = 'Bob' WHERE id = 1";
        $result = $transformer->buildProjection($sql, 'users', ['id', 'name']);

        self::assertArrayHasKey('tables', $result);
        self::assertCount(1, $result['tables']);
        self::assertArrayHasKey('users', $result['tables']);
        self::assertSame('users', $result['tables']['users']['alias']);
    }

    public function testTransformAppliesCteShadowing(): void
    {
        $transformer = new UpdateTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = "UPDATE users SET name = 'Bob' WHERE id = 1";
        $tables = [
            'users' => [
                'rows' => [['id' => 1, 'name' => 'Alice']],
                'columns' => ['id', 'name'],
                'columnTypes' => [
                    'id' => new ColumnDeclaration(ColumnTypeFamily::INTEGER, 'INTEGER'),
                    'name' => new ColumnDeclaration(ColumnTypeFamily::TEXT, 'TEXT'),
                ],
                'primaryKeys' => ['id'],
            ],
        ];

        $result = $transformer->transform($sql, $tables);
        self::assertStringContainsString('WITH', $result);
        self::assertStringContainsString('"users" AS MATERIALIZED', $result);
        self::assertStringContainsString('"users"."id" AS "__ztd_original_id"', $result);
    }

    public function testTransformThrowsOnUnresolvableTarget(): void
    {
        $transformer = new UpdateTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = 'UPDATE';
        $this->expectException(UnsupportedSqlException::class);
        $transformer->transform($sql, []);
    }

    public function testBuildProjectionUsesDoubleQuoteIdentifiers(): void
    {
        $transformer = new UpdateTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = "UPDATE users SET name = 'Bob' WHERE id = 1";
        $result = $transformer->buildProjection($sql, 'users', ['id', 'name']);

        self::assertStringContainsString('"users"', $result['sql']);
        self::assertStringContainsString('"id"', $result['sql']);
        self::assertStringContainsString('"name"', $result['sql']);
        self::assertStringNotContainsString('`', $result['sql']);
    }

    public function testBuildProjectionExactOutputSimple(): void
    {
        $transformer = new UpdateTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = "UPDATE users SET name = 'Bob' WHERE id = 1";
        $result = $transformer->buildProjection($sql, 'users', ['id', 'name']);

        self::assertSame(
            "SELECT 'Bob' AS \"name\", \"users\".\"id\" FROM \"users\" WHERE id = 1",
            $result['sql']
        );
    }

    public function testBuildProjectionExactOutputWithAlias(): void
    {
        $transformer = new UpdateTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = "UPDATE users u SET name = 'Bob' WHERE u.id = 1";
        $result = $transformer->buildProjection($sql, 'users', ['id', 'name']);

        self::assertSame(
            "SELECT 'Bob' AS \"name\", \"u\".\"id\" FROM \"users\" AS \"u\" WHERE u.id = 1",
            $result['sql']
        );
    }

    public function testBuildProjectionOnlySetColumnsWhenNoTableColumns(): void
    {
        $transformer = new UpdateTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = "UPDATE users SET name = 'Bob', email = 'b@x.com' WHERE id = 1";
        $result = $transformer->buildProjection($sql, 'users', []);

        self::assertSame(
            "SELECT 'Bob' AS \"name\", 'b@x.com' AS \"email\" FROM \"users\" WHERE id = 1",
            $result['sql']
        );
    }

    public function testBuildProjectionWildcardFallback(): void
    {
        $transformer = new UpdateTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = 'UPDATE users SET active = true';
        $result = $transformer->buildProjection($sql, 'users', []);

        self::assertSame(
            'SELECT true AS "active" FROM "users"',
            $result['sql']
        );
    }

    public function testBuildProjectionWithFromClauseExact(): void
    {
        $transformer = new UpdateTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = 'UPDATE users SET name = orders.name FROM orders WHERE users.id = orders.user_id';
        $result = $transformer->buildProjection($sql, 'users', ['id', 'name']);

        self::assertSame(
            'SELECT orders.name AS "name", "users"."id" FROM "users", orders WHERE users.id = orders.user_id',
            $result['sql']
        );
    }

    public function testTransformWithEmptyTableContext(): void
    {
        $transformer = new UpdateTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = "UPDATE users SET name = 'Bob' WHERE id = 1";
        $tables = [
            'users' => [
                'rows' => [],
                'columns' => ['id', 'name'],
                'columnTypes' => [
                    'id' => new ColumnDeclaration(ColumnTypeFamily::INTEGER, 'INTEGER'),
                    'name' => new ColumnDeclaration(ColumnTypeFamily::TEXT, 'TEXT'),
                ],
            ],
        ];

        $result = $transformer->transform($sql, $tables);
        self::assertStringContainsString('WHERE FALSE', $result);
        self::assertStringContainsString('"users" AS MATERIALIZED', $result);
    }

    public function testBuildProjectionSetColumnExcludesFromRemainingColumns(): void
    {
        $transformer = new UpdateTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = "UPDATE users SET name = 'Bob', email = 'b@x.com' WHERE id = 1";
        $result = $transformer->buildProjection($sql, 'users', ['id', 'name', 'email']);

        self::assertSame(
            "SELECT 'Bob' AS \"name\", 'b@x.com' AS \"email\", \"users\".\"id\" FROM \"users\" WHERE id = 1",
            $result['sql']
        );
    }

    public function testBuildProjectionCoalesceColumnsFromTableContext(): void
    {
        $transformer = new UpdateTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = "UPDATE users SET name = 'Bob' WHERE id = 1";
        $tables = [
            'users' => [
                'rows' => [['id' => 1, 'name' => 'Alice', 'email' => 'a@b.com']],
                'columns' => ['id', 'name', 'email'],
                'columnTypes' => [
                    'id' => new ColumnDeclaration(ColumnTypeFamily::INTEGER, 'INTEGER'),
                    'name' => new ColumnDeclaration(ColumnTypeFamily::TEXT, 'TEXT'),
                    'email' => new ColumnDeclaration(ColumnTypeFamily::TEXT, 'TEXT'),
                ],
            ],
        ];

        $result = $transformer->transform($sql, $tables);
        self::assertStringContainsString("'Bob' AS \"name\"", $result);
        self::assertStringContainsString('"users"."id"', $result);
        self::assertStringContainsString('"users"."email"', $result);
    }

    public function testBuildProjectionSetColumnIsCoveredNotDuplicated(): void
    {
        $transformer = new UpdateTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = "UPDATE users SET name = 'Bob' WHERE id = 1";
        $result = $transformer->buildProjection($sql, 'users', ['id', 'name']);

        self::assertSame(1, substr_count($result['sql'], '"name"'));
    }
}
