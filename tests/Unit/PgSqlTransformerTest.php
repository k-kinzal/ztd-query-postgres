<?php

declare(strict_types=1);

namespace Tests\Unit;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Exception\UnsupportedSqlException;
use ZtdQuery\Platform\Postgres\PgSqlCastRenderer;
use ZtdQuery\Platform\Postgres\PgSqlIdentifierQuoter;
use ZtdQuery\Platform\Postgres\PgSqlParser;
use ZtdQuery\Platform\Postgres\PgSqlTransformer;
use ZtdQuery\Platform\Postgres\Transformer\DeleteTransformer;
use ZtdQuery\Platform\Postgres\Transformer\InsertTransformer;
use ZtdQuery\Platform\Postgres\Transformer\MergeTransformer;
use ZtdQuery\Platform\Postgres\Transformer\SelectTransformer;
use ZtdQuery\Platform\Postgres\Transformer\UpdateTransformer;
use ZtdQuery\Schema\ColumnType;
use ZtdQuery\Schema\ColumnTypeFamily;
use ZtdQuery\Schema\IdentityGenerationStrategy;

#[CoversClass(PgSqlTransformer::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlSelectRelationParser::class)]
#[UsesClass(PgSqlParser::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PostgreSqlLexicalMasker::class)]
#[UsesClass(SelectTransformer::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlTableSampleParser::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlTableSampleRewriter::class)]
#[UsesClass(InsertTransformer::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Transformer\InsertRowRenderer::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Transformer\InsertSelectRenderer::class)]
#[UsesClass(MergeTransformer::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlMergeParser::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlMergeStatement::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlMergeClause::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlMergeMatchKind::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlMergeActionKind::class)]
#[UsesClass(UpdateTransformer::class)]
#[UsesClass(DeleteTransformer::class)]
#[UsesClass(PgSqlCastRenderer::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlValueRenderer::class)]
#[UsesClass(PgSqlIdentifierQuoter::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlCteShadowComposer::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlNativeUpsertProjector::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlGeneratedColumnProjector::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlLexerProfile::class)]
final class PgSqlTransformerTest extends TestCase
{
    public function testTransformSelectDelegatesToSelectTransformer(): void
    {
        $parser = new PgSqlParser();
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $result = $transformer->transform('SELECT * FROM users', ['users' => ['alias' => '"users"', 'rows' => [['id' => 1, 'name' => 'Alice']], 'columns' => ['id', 'name'], 'columnTypes' => ['id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'), 'name' => new ColumnType(ColumnTypeFamily::STRING, 'TEXT')]]]);
        self::assertStringContainsString('WITH', $result);
    }

    public function testTransformInsertDelegatesToInsertTransformer(): void
    {
        $parser = new PgSqlParser();
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $result = $transformer->transform("INSERT INTO users (id, name) VALUES (1, 'Alice')", ['users' => ['alias' => '"users"', 'rows' => [], 'columns' => ['id', 'name'], 'columnTypes' => ['id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'), 'name' => new ColumnType(ColumnTypeFamily::STRING, 'TEXT')]]]);
        self::assertNotEmpty($result);
    }

    public function testTransformUpdateDelegatesToUpdateTransformer(): void
    {
        $parser = new PgSqlParser();
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $result = $transformer->transform("UPDATE users SET name = 'Bob' WHERE id = 1", ['users' => ['alias' => '"users"', 'rows' => [['id' => 1, 'name' => 'Alice']], 'columns' => ['id', 'name'], 'columnTypes' => ['id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'), 'name' => new ColumnType(ColumnTypeFamily::STRING, 'TEXT')]]]);
        self::assertNotEmpty($result);
    }

    public function testTransformDeleteDelegatesToDeleteTransformer(): void
    {
        $parser = new PgSqlParser();
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $result = $transformer->transform('DELETE FROM users WHERE id = 1', ['users' => ['alias' => '"users"', 'rows' => [['id' => 1, 'name' => 'Alice']], 'columns' => ['id', 'name'], 'columnTypes' => ['id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'), 'name' => new ColumnType(ColumnTypeFamily::STRING, 'TEXT')]]]);
        self::assertNotEmpty($result);
    }

    public function testTransformMergeDelegatesToMergeTransformer(): void
    {
        $parser = new PgSqlParser();
        $selectTransformer = new SelectTransformer();
        $transformer = new PgSqlTransformer(
            $parser,
            $selectTransformer,
            new InsertTransformer($parser, $selectTransformer),
            new UpdateTransformer($parser, $selectTransformer),
            new DeleteTransformer($parser, $selectTransformer),
        );
        $result = $transformer->transform(
            'MERGE INTO users u USING source s ON u.id = s.id WHEN MATCHED THEN DELETE',
            [
                'users' => [
                    'rows' => [['id' => 1]],
                    'columns' => ['id'],
                    'columnTypes' => ['id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER')],
                ],
                'source' => [
                    'rows' => [['id' => 1]],
                    'columns' => ['id'],
                    'columnTypes' => ['id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER')],
                ],
            ],
        );

        self::assertStringContainsString('WHERE NOT (EXISTS', $result);
        $transformer->commitRewriteState();
    }

    public function testCommitRewriteStateCommitsGeneratedIdentityValues(): void
    {
        $parser = new PgSqlParser();
        $selectTransformer = new SelectTransformer();
        $transformer = new PgSqlTransformer(
            $parser,
            $selectTransformer,
            new InsertTransformer($parser, $selectTransformer),
            new UpdateTransformer($parser, $selectTransformer),
            new DeleteTransformer($parser, $selectTransformer),
        );
        $tables = ['users' => [
            'rows' => [],
            'columns' => ['id', 'name'],
            'columnTypes' => [],
            'identityStrategies' => ['id' => IdentityGenerationStrategy::Sequence],
        ]];

        $first = $transformer->transform("INSERT INTO users (name) VALUES ('first')", $tables);
        $transformer->commitRewriteState();
        $second = $transformer->transform("INSERT INTO users (name) VALUES ('second')", $tables);

        self::assertStringContainsString('1 AS "id"', $first);
        self::assertStringContainsString('2 AS "id"', $second);
    }

    public function testTransformUnsupportedStatementThrows(): void
    {
        $parser = new PgSqlParser();
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $this->expectException(UnsupportedSqlException::class);
        $transformer->transform('CREATE TABLE test (id INTEGER)', []);
    }
}
