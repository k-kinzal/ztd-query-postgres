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
use ZtdQuery\Platform\Postgres\Transformer\SelectTransformer;
use ZtdQuery\Platform\Postgres\Transformer\UpdateTransformer;
use ZtdQuery\Schema\ColumnType;
use ZtdQuery\Schema\ColumnTypeFamily;

#[CoversClass(PgSqlTransformer::class)]
#[UsesClass(PgSqlParser::class)]
#[UsesClass(SelectTransformer::class)]
#[UsesClass(InsertTransformer::class)]
#[UsesClass(UpdateTransformer::class)]
#[UsesClass(DeleteTransformer::class)]
#[UsesClass(PgSqlCastRenderer::class)]
#[UsesClass(PgSqlIdentifierQuoter::class)]
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
