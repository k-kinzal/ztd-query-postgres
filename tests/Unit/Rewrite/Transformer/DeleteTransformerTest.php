<?php

declare(strict_types=1);

namespace Tests\Unit\Rewrite\Transformer;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Exception\UnsupportedSqlException;
use ZtdQuery\Platform\Postgres\Rewrite\Transformer\DeleteTransformer;
use ZtdQuery\Platform\Postgres\Rewrite\Transformer\SelectTransformer;
use ZtdQuery\Platform\Postgres\Sql\PgSqlIdentifierQuoter;
use ZtdQuery\Platform\Postgres\Sql\PgSqlParser;
use ZtdQuery\Platform\Postgres\Sql\Value\PgSqlCastRenderer;
use ZtdQuery\Schema\ColumnDeclaration;
use ZtdQuery\Schema\ColumnTypeFamily;

#[CoversClass(DeleteTransformer::class)]
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
final class DeleteTransformerTest extends TestCase
{
    public function testBuildProjectionSimpleDelete(): void
    {
        $transformer = new DeleteTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = 'DELETE FROM users WHERE id = 1';
        $result = $transformer->buildProjection($sql, 'users', ['id', 'name']);

        self::assertSame('users', $result['table']);
        self::assertStringContainsString('"users"."id" AS "id"', $result['sql']);
        self::assertStringContainsString('"users"."name" AS "name"', $result['sql']);
        self::assertStringContainsString('FROM "users"', $result['sql']);
        self::assertStringContainsString('WHERE id = 1', $result['sql']);
    }

    public function testBuildProjectionWithAlias(): void
    {
        $transformer = new DeleteTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = 'DELETE FROM users u WHERE u.id = 1';
        $result = $transformer->buildProjection($sql, 'users', ['id', 'name']);

        self::assertSame('users', $result['table']);
        self::assertStringContainsString('AS "u"', $result['sql']);
        self::assertStringContainsString('"u"."id" AS "id"', $result['sql']);
        self::assertStringContainsString('"u"."name" AS "name"', $result['sql']);
    }

    public function testBuildProjectionWithUsingClause(): void
    {
        $transformer = new DeleteTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = 'DELETE FROM users USING orders WHERE users.id = orders.user_id AND orders.status = \'canceled\'';
        $result = $transformer->buildProjection($sql, 'users', ['id', 'name']);

        self::assertStringContainsString('orders', $result['sql']);
        self::assertStringContainsString('WHERE', $result['sql']);
    }

    public function testBuildProjectionWithNoWhereClause(): void
    {
        $transformer = new DeleteTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = 'DELETE FROM users';
        $result = $transformer->buildProjection($sql, 'users', ['id', 'name']);

        self::assertStringContainsString('SELECT', $result['sql']);
        self::assertStringContainsString('FROM "users"', $result['sql']);
        self::assertStringNotContainsString('WHERE', $result['sql']);
    }

    public function testBuildProjectionWithEmptyColumnsUsesWildcard(): void
    {
        $transformer = new DeleteTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = 'DELETE FROM users WHERE id = 1';
        $result = $transformer->buildProjection($sql, 'users', []);

        self::assertStringContainsString('"users".*', $result['sql']);
    }

    public function testBuildProjectionReturnsTablesArray(): void
    {
        $transformer = new DeleteTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = 'DELETE FROM users WHERE id = 1';
        $result = $transformer->buildProjection($sql, 'users', ['id']);

        self::assertArrayHasKey('tables', $result);
        self::assertCount(1, $result['tables']);
        self::assertArrayHasKey('users', $result['tables']);
        self::assertSame('users', $result['tables']['users']['alias']);
    }

    public function testBuildProjectionWithAliasReturnsAliasInTables(): void
    {
        $transformer = new DeleteTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = 'DELETE FROM users u WHERE u.id = 1';
        $result = $transformer->buildProjection($sql, 'users', ['id']);

        self::assertArrayHasKey('users', $result['tables']);
        self::assertSame('u', $result['tables']['users']['alias']);
    }

    public function testTransformAppliesCteShadowing(): void
    {
        $transformer = new DeleteTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = 'DELETE FROM users WHERE id = 1';
        $tables = [
            'users' => [
                'rows' => [['id' => 1, 'name' => 'Alice']],
                'columns' => ['id', 'name'],
                'columnTypes' => [
                    'id' => new ColumnDeclaration(ColumnTypeFamily::INTEGER, 'INTEGER'),
                    'name' => new ColumnDeclaration(ColumnTypeFamily::TEXT, 'TEXT'),
                ],
            ],
        ];

        $result = $transformer->transform($sql, $tables);
        self::assertStringContainsString('WITH', $result);
        self::assertStringContainsString('"users" AS MATERIALIZED', $result);
    }

    public function testTransformThrowsOnUnresolvableTarget(): void
    {
        $transformer = new DeleteTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = 'DELETE FROM';
        $this->expectException(UnsupportedSqlException::class);
        $transformer->transform($sql, []);
    }

    public function testBuildProjectionUsesDoubleQuoteIdentifiers(): void
    {
        $transformer = new DeleteTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = 'DELETE FROM users WHERE id = 1';
        $result = $transformer->buildProjection($sql, 'users', ['id', 'name']);

        self::assertStringContainsString('"users"', $result['sql']);
        self::assertStringContainsString('"id"', $result['sql']);
        self::assertStringContainsString('"name"', $result['sql']);
        self::assertStringNotContainsString('`', $result['sql']);
    }

    public function testBuildProjectionExactOutputSimple(): void
    {
        $transformer = new DeleteTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = 'DELETE FROM users WHERE id = 1';
        $result = $transformer->buildProjection($sql, 'users', ['id', 'name']);

        self::assertSame(
            'SELECT "users"."id" AS "id", "users"."name" AS "name" FROM "users" WHERE id = 1',
            $result['sql']
        );
    }

    public function testBuildProjectionExactOutputWithAlias(): void
    {
        $transformer = new DeleteTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = 'DELETE FROM users u WHERE u.id = 1';
        $result = $transformer->buildProjection($sql, 'users', ['id', 'name']);

        self::assertSame(
            'SELECT "u"."id" AS "id", "u"."name" AS "name" FROM "users" AS "u" WHERE u.id = 1',
            $result['sql']
        );
    }

    public function testBuildProjectionExactOutputWithUsing(): void
    {
        $transformer = new DeleteTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = "DELETE FROM users USING orders WHERE users.id = orders.user_id AND orders.status = 'canceled'";
        $result = $transformer->buildProjection($sql, 'users', ['id', 'name']);

        self::assertSame(
            "SELECT \"users\".\"id\" AS \"id\", \"users\".\"name\" AS \"name\" FROM \"users\", orders WHERE users.id = orders.user_id AND orders.status = 'canceled'",
            $result['sql']
        );
    }

    public function testBuildProjectionEmptyColumnsWildcard(): void
    {
        $transformer = new DeleteTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = 'DELETE FROM users WHERE id = 1';
        $result = $transformer->buildProjection($sql, 'users', []);

        self::assertSame(
            'SELECT "users".* FROM "users" WHERE id = 1',
            $result['sql']
        );
    }

    public function testBuildProjectionNoWhere(): void
    {
        $transformer = new DeleteTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = 'DELETE FROM users';
        $result = $transformer->buildProjection($sql, 'users', ['id']);

        self::assertSame(
            'SELECT "users"."id" AS "id" FROM "users"',
            $result['sql']
        );
    }

    public function testTransformWithEmptyTableContext(): void
    {
        $transformer = new DeleteTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = 'DELETE FROM users WHERE id = 1';
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

    public function testTransformCoalesceUsesTableContextColumns(): void
    {
        $transformer = new DeleteTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = 'DELETE FROM users WHERE id = 1';
        $tables = [
            'users' => [
                'rows' => [['id' => 1, 'name' => 'Alice']],
                'columns' => ['id', 'name'],
                'columnTypes' => [
                    'id' => new ColumnDeclaration(ColumnTypeFamily::INTEGER, 'INTEGER'),
                    'name' => new ColumnDeclaration(ColumnTypeFamily::TEXT, 'TEXT'),
                ],
            ],
        ];

        $result = $transformer->transform($sql, $tables);
        self::assertStringContainsString('"users"."id" AS "id"', $result);
        self::assertStringContainsString('"users"."name" AS "name"', $result);
    }

    public function testTransformWithoutColumnsInContextUsesWildcard(): void
    {
        $transformer = new DeleteTransformer(new PgSqlParser(), new SelectTransformer());
        $sql = 'DELETE FROM users WHERE id = 1';
        $tables = [];

        $result = $transformer->transform($sql, $tables);
        self::assertStringContainsString('"users".*', $result);
    }
}
