<?php

declare(strict_types=1);

namespace Tests\Unit;

use Tests\Contract\RewriterContractTest;
use ZtdQuery\Platform\SchemaParser;
use ZtdQuery\Rewrite\SqlRewriter;
use ZtdQuery\Exception\UnsupportedSqlException;
use ZtdQuery\Platform\Postgres\PgSqlCastRenderer;
use ZtdQuery\Platform\Postgres\PgSqlIdentifierQuoter;
use ZtdQuery\Platform\Postgres\PgSqlMutationResolver;
use ZtdQuery\Platform\Postgres\PgSqlParser;
use ZtdQuery\Platform\Postgres\PgSqlQueryGuard;
use ZtdQuery\Platform\Postgres\PgSqlRewriter;
use ZtdQuery\Platform\Postgres\PgSqlSchemaParser;
use ZtdQuery\Platform\Postgres\PgSqlTransformer;
use ZtdQuery\Platform\Postgres\Transformer\DeleteTransformer;
use ZtdQuery\Platform\Postgres\Transformer\InsertTransformer;
use ZtdQuery\Platform\Postgres\Transformer\SelectTransformer;
use ZtdQuery\Platform\Postgres\Transformer\UpdateTransformer;
use ZtdQuery\Rewrite\QueryKind;
use ZtdQuery\Schema\ColumnType;
use ZtdQuery\Schema\ColumnTypeFamily;
use ZtdQuery\Schema\TableDefinition;
use ZtdQuery\Schema\TableDefinitionRegistry;
use ZtdQuery\Shadow\Mutation\CreateTableMutation;
use ZtdQuery\Shadow\Mutation\DeleteMutation;
use ZtdQuery\Shadow\Mutation\DropTableMutation;
use ZtdQuery\Shadow\Mutation\InsertMutation;
use ZtdQuery\Shadow\Mutation\TruncateMutation;
use ZtdQuery\Shadow\Mutation\UpdateMutation;
use ZtdQuery\Shadow\ShadowStore;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;

#[CoversClass(PgSqlRewriter::class)]
#[UsesClass(PgSqlParser::class)]
#[UsesClass(PgSqlSchemaParser::class)]
#[UsesClass(PgSqlQueryGuard::class)]
#[UsesClass(PgSqlMutationResolver::class)]
#[UsesClass(PgSqlTransformer::class)]
#[UsesClass(SelectTransformer::class)]
#[UsesClass(InsertTransformer::class)]
#[UsesClass(UpdateTransformer::class)]
#[UsesClass(DeleteTransformer::class)]
#[UsesClass(PgSqlCastRenderer::class)]
#[UsesClass(PgSqlIdentifierQuoter::class)]
final class PgSqlRewriterTest extends RewriterContractTest
{
    protected function createRewriter(ShadowStore $store, TableDefinitionRegistry $registry): SqlRewriter
    {
        $parser = new PgSqlParser();
        $schemaParser = new PgSqlSchemaParser();
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $mutationResolver = new PgSqlMutationResolver($store, $registry, $schemaParser, $parser);

        return new PgSqlRewriter(new PgSqlQueryGuard($parser), $store, $registry, $transformer, $mutationResolver, $parser);
    }

    protected function createSchemaParser(): SchemaParser
    {
        return new PgSqlSchemaParser();
    }

    protected function selectSql(): string
    {
        return 'SELECT id, name, email FROM users WHERE id = 1';
    }

    protected function insertSql(): string
    {
        return "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')";
    }

    protected function updateSql(): string
    {
        return "UPDATE users SET name = 'Bob' WHERE id = 1";
    }

    protected function deleteSql(): string
    {
        return 'DELETE FROM users WHERE id = 1';
    }

    protected function createTableSql(): string
    {
        return 'CREATE TABLE orders (id INTEGER PRIMARY KEY, amount NUMERIC(10,2))';
    }

    protected function dropTableSql(): string
    {
        return 'DROP TABLE IF EXISTS orders';
    }

    protected function unsupportedSql(): string
    {
        return 'CREATE DATABASE test_db';
    }

    protected function usersCreateTableSql(): string
    {
        return <<<'SQL'
            CREATE TABLE users (
                id INTEGER NOT NULL,
                name VARCHAR(255) NOT NULL,
                email VARCHAR(255) NOT NULL,
                PRIMARY KEY (id)
            )
            SQL;
    }

    public function testSelectReturnsReadKind(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $plan = $rewriter->rewrite('SELECT * FROM users');
        self::assertSame(QueryKind::READ, $plan->kind());
        self::assertNull($plan->mutation());
    }

    public function testSelectTransformsCteWithShadowData(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $shadowStore->set('users', [
            ['id' => 1, 'name' => 'Alice', 'email' => 'alice@example.com'],
        ]);

        $plan = $rewriter->rewrite('SELECT * FROM users');
        self::assertSame(QueryKind::READ, $plan->kind());
        self::assertStringStartsWith('WITH', $plan->sql(), 'CTE-shadowed SELECT must start with WITH');
        self::assertStringContainsString('AS MATERIALIZED', $plan->sql());
        self::assertStringContainsString('"users"', $plan->sql());
    }

    public function testInsertReturnsWriteSimulatedWithMutation(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $plan = $rewriter->rewrite("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'a@b.com')");
        self::assertSame(QueryKind::WRITE_SIMULATED, $plan->kind());
        self::assertNotNull($plan->mutation());
        self::assertInstanceOf(InsertMutation::class, $plan->mutation());
    }

    public function testUpdateReturnsWriteSimulatedWithMutation(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $plan = $rewriter->rewrite("UPDATE users SET name = 'Bob' WHERE id = 1");
        self::assertSame(QueryKind::WRITE_SIMULATED, $plan->kind());
        self::assertNotNull($plan->mutation());
        self::assertInstanceOf(UpdateMutation::class, $plan->mutation());
    }

    public function testDeleteReturnsWriteSimulatedWithMutation(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $plan = $rewriter->rewrite('DELETE FROM users WHERE id = 1');
        self::assertSame(QueryKind::WRITE_SIMULATED, $plan->kind());
        self::assertNotNull($plan->mutation());
        self::assertInstanceOf(DeleteMutation::class, $plan->mutation());
    }

    public function testTruncateReturnsWriteSimulatedWithMutation(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $plan = $rewriter->rewrite('TRUNCATE TABLE users');
        self::assertSame(QueryKind::WRITE_SIMULATED, $plan->kind());
        self::assertNotNull($plan->mutation());
        self::assertInstanceOf(TruncateMutation::class, $plan->mutation());
    }

    public function testCreateTableReturnsDdlSimulated(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $plan = $rewriter->rewrite('CREATE TABLE orders (id INTEGER PRIMARY KEY, total NUMERIC(10,2))');
        self::assertSame(QueryKind::DDL_SIMULATED, $plan->kind());
        self::assertNotNull($plan->mutation());
        self::assertInstanceOf(CreateTableMutation::class, $plan->mutation());
    }

    public function testDropTableReturnsDdlSimulated(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $plan = $rewriter->rewrite('DROP TABLE users');
        self::assertSame(QueryKind::DDL_SIMULATED, $plan->kind());
        self::assertNotNull($plan->mutation());
        self::assertInstanceOf(DropTableMutation::class, $plan->mutation());
    }

    public function testUnsupportedSqlThrowsException(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $this->expectException(UnsupportedSqlException::class);
        $rewriter->rewrite('CREATE DATABASE test');
    }

    public function testEmptyInputThrowsException(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $this->expectException(UnsupportedSqlException::class);
        $rewriter->rewrite('');
    }

    public function testMultiStatementThrowsException(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $this->expectException(UnsupportedSqlException::class);
        $rewriter->rewrite('SELECT 1; SELECT 2');
    }

    public function testRewriteIsDeterministic(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $sql = 'SELECT * FROM users WHERE id = 1';
        $plan1 = $rewriter->rewrite($sql);
        $plan2 = $rewriter->rewrite($sql);

        self::assertSame($plan1->sql(), $plan2->sql());
        self::assertSame($plan1->kind(), $plan2->kind());
    }

    public function testReadPlanHasNoMutation(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $plan = $rewriter->rewrite('SELECT * FROM users');
        self::assertNull($plan->mutation());
    }

    public function testWritePlanHasNonNullMutation(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $plan = $rewriter->rewrite("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'a@b.com')");
        self::assertNotNull($plan->mutation());
    }

    public function testRewriteMultiple(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $multiPlan = $rewriter->rewriteMultiple("SELECT * FROM users; INSERT INTO users (id, name, email) VALUES (2, 'Bob', 'b@c.com')");
        self::assertSame(2, $multiPlan->count());
        self::assertSame(QueryKind::READ, $multiPlan->get(0)?->kind());
        self::assertSame(QueryKind::WRITE_SIMULATED, $multiPlan->get(1)?->kind());
    }

    public function testRewriteMultipleEmpty(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $this->expectException(UnsupportedSqlException::class);
        $rewriter->rewriteMultiple('');
    }

    public function testInsertResultSelectContainsColumnNames(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $plan = $rewriter->rewrite("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'a@b.com')");
        $sql = $plan->sql();
        self::assertMatchesRegularExpression('/^(?:WITH\b|SELECT\b)/i', $sql, 'INSERT result-select must start with SELECT or WITH...SELECT');
        self::assertStringContainsString('"id"', $sql);
        self::assertStringContainsString('"name"', $sql);
        self::assertStringContainsString('"email"', $sql);
    }

    public function testSelectWithEmptyShadowGeneratesEmptyCte(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $shadowStore->ensure('users');

        $plan = $rewriter->rewrite('SELECT * FROM users');
        $sql = $plan->sql();
        self::assertStringStartsWith('WITH', $sql, 'Empty shadow CTE must start with WITH');
        self::assertStringContainsString('WHERE FALSE', $sql);
        self::assertStringContainsString('AS MATERIALIZED', $sql);
    }

    public function testSelectWithMultiRowShadowUsesValues(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $shadowStore->set('users', [
            ['id' => 1, 'name' => 'Alice', 'email' => 'alice@example.com'],
            ['id' => 2, 'name' => 'Bob', 'email' => 'bob@example.com'],
        ]);

        $plan = $rewriter->rewrite('SELECT * FROM users');
        $sql = $plan->sql();
        self::assertStringStartsWith('WITH', $sql, 'Multi-row shadow CTE must start with WITH');
        self::assertStringContainsString('VALUES', $sql);
        self::assertStringContainsString('AS MATERIALIZED', $sql);
    }

    public function testRewriteWithLeadingWhitespace(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $plan = $rewriter->rewrite('  SELECT * FROM users  ');
        self::assertSame(QueryKind::READ, $plan->kind());
    }

    public function testRewriteWhitespaceOnlyThrows(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $this->expectException(UnsupportedSqlException::class);
        $rewriter->rewrite('   ');
    }

    public function testRewriteMultipleWhitespaceOnlyThrows(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $this->expectException(UnsupportedSqlException::class);
        $rewriter->rewriteMultiple('   ');
    }

    public function testRewriteEmptyInputForRewriteMultipleThrows(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $this->expectException(UnsupportedSqlException::class);
        $rewriter->rewriteMultiple('');
    }

    public function testSelectUnknownTableWithSchemaContextThrows(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $this->expectException(\ZtdQuery\Exception\UnknownSchemaException::class);
        $rewriter->rewrite('SELECT * FROM nonexistent_table');
    }

    public function testSelectWithNoSchemaContextPassesThrough(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);

        $plan = $rewriter->rewrite('SELECT * FROM anything');
        self::assertSame(QueryKind::READ, $plan->kind());
        self::assertSame('SELECT * FROM anything', $plan->sql());
    }

    public function testSelectTableExistsInShadowStoreNotRegistry(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $shadowStore->set('temp_table', [['id' => 1, 'val' => 'x']]);
        $plan = $rewriter->rewrite('SELECT * FROM temp_table');
        self::assertSame(QueryKind::READ, $plan->kind());
        self::assertStringContainsString('"temp_table" AS MATERIALIZED', $plan->sql());
    }

    public function testTruncateReturnsSelectOneWhereFalse(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $plan = $rewriter->rewrite('TRUNCATE TABLE users');
        self::assertSame('SELECT 1 WHERE FALSE', $plan->sql());
        self::assertSame(QueryKind::WRITE_SIMULATED, $plan->kind());
    }

    public function testCreateTableReturnsSelectOneWhereFalse(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $plan = $rewriter->rewrite('CREATE TABLE orders (id INTEGER PRIMARY KEY, total NUMERIC(10,2))');
        self::assertSame('SELECT 1 WHERE FALSE', $plan->sql());
        self::assertSame(QueryKind::DDL_SIMULATED, $plan->kind());
    }

    public function testDropTableReturnsSelectOneWhereFalse(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $plan = $rewriter->rewrite('DROP TABLE users');
        self::assertSame('SELECT 1 WHERE FALSE', $plan->sql());
        self::assertSame(QueryKind::DDL_SIMULATED, $plan->kind());
    }

    public function testCreateTableAsSelectReturnsDdlWithTransformedSql(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $shadowStore->set('users', [['id' => 1, 'name' => 'Alice', 'email' => 'alice@example.com']]);

        $plan = $rewriter->rewrite('CREATE TABLE archive AS SELECT id, name FROM users');
        self::assertSame(QueryKind::DDL_SIMULATED, $plan->kind());
        self::assertStringContainsString('"users" AS MATERIALIZED', $plan->sql());
        self::assertStringContainsString('SELECT id, name FROM users', $plan->sql());
    }

    public function testUpdateEnsuresDmlTargetViaShadowStore(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $rewriter->rewrite("UPDATE users SET name = 'Bob' WHERE id = 1");
        self::assertSame([], $shadowStore->get('users'));
    }

    public function testDeleteEnsuresDmlTargetViaShadowStore(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $rewriter->rewrite('DELETE FROM users WHERE id = 1');
        self::assertSame([], $shadowStore->get('users'));
    }

    public function testInsertSqlIsTransformed(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $plan = $rewriter->rewrite("INSERT INTO users (id, name, email) VALUES (2, 'New', 'new@ex.com')");
        self::assertSame(QueryKind::WRITE_SIMULATED, $plan->kind());
        self::assertStringContainsString('SELECT', $plan->sql());
        self::assertStringContainsString('"id"', $plan->sql());
        self::assertStringContainsString('"name"', $plan->sql());
        self::assertStringContainsString('"email"', $plan->sql());
    }

    public function testUpdateSqlIsTransformed(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $shadowStore->set('users', [['id' => 1, 'name' => 'Alice', 'email' => 'alice@ex.com']]);
        $plan = $rewriter->rewrite("UPDATE users SET name = 'Bob' WHERE id = 1");
        self::assertSame(QueryKind::WRITE_SIMULATED, $plan->kind());
        self::assertStringContainsString('"users" AS MATERIALIZED', $plan->sql());
    }

    public function testDeleteSqlIsTransformed(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $shadowStore->set('users', [['id' => 1, 'name' => 'Alice', 'email' => 'alice@ex.com']]);
        $plan = $rewriter->rewrite('DELETE FROM users WHERE id = 1');
        self::assertSame(QueryKind::WRITE_SIMULATED, $plan->kind());
        self::assertStringContainsString('"users" AS MATERIALIZED', $plan->sql());
    }

    public function testRewriteMultipleAllReadStatements(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $multiPlan = $rewriter->rewriteMultiple('SELECT 1; SELECT 2');
        self::assertSame(2, $multiPlan->count());
        self::assertSame(QueryKind::READ, $multiPlan->get(0)?->kind());
        self::assertSame(QueryKind::READ, $multiPlan->get(1)?->kind());
    }

    public function testBuildTableContextMergesRegistryAndShadow(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $shadowStore->set('users', [['id' => 1, 'name' => 'Alice', 'email' => 'alice@ex.com']]);

        $plan = $rewriter->rewrite('SELECT * FROM users');
        self::assertSame(QueryKind::READ, $plan->kind());
        self::assertStringContainsString('"users" AS MATERIALIZED', $plan->sql());
        self::assertStringContainsString("'Alice'", $plan->sql());
    }

    public function testBuildTableContextRegistryOnlyNoShadow(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $plan = $rewriter->rewrite('SELECT * FROM users');
        self::assertSame(QueryKind::READ, $plan->kind());
        self::assertStringContainsString('"users" AS MATERIALIZED', $plan->sql());
        self::assertStringContainsString('WHERE FALSE', $plan->sql());
    }

    public function testSelectWithShadowOnlyDataDerivesColumns(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $shadowStore->set('temp', [['a' => 1, 'b' => 'x']]);
        $plan = $rewriter->rewrite('SELECT * FROM temp');
        self::assertSame(QueryKind::READ, $plan->kind());
        self::assertStringContainsString('"temp" AS MATERIALIZED', $plan->sql());
    }

    public function testSkippedStatementPassesThrough(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $plan = $rewriter->rewrite('BEGIN');
        self::assertSame(QueryKind::SKIPPED, $plan->kind());
        self::assertSame('BEGIN', $plan->sql());
        self::assertNull($plan->mutation());
    }

    public function testSelectWithShadowOnlyDataMergesColumnsAcrossRows(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $shadowStore->set('temp', [
            ['a' => 1, 'b' => 2],
            ['a' => 3, 'c' => 4],
        ]);
        $plan = $rewriter->rewrite('SELECT * FROM temp');
        self::assertSame(QueryKind::READ, $plan->kind());
        self::assertStringContainsString('"temp" AS MATERIALIZED', $plan->sql());
    }

    public function testUpdateEnsuresDmlTargetInShadowStore(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('products', new TableDefinition(
            ['id', 'name'],
            ['id' => 'INTEGER', 'name' => 'TEXT'],
            ['id'],
            ['id'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);

        $rewriter->rewrite("UPDATE products SET name = 'New' WHERE id = 1");
        self::assertSame([], $shadowStore->get('products'));
    }

    public function testDeleteEnsuresDmlTargetInShadowStore(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('products', new TableDefinition(
            ['id', 'name'],
            ['id' => 'INTEGER', 'name' => 'TEXT'],
            ['id'],
            ['id'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);

        $rewriter->rewrite('DELETE FROM products WHERE id = 1');
        self::assertSame([], $shadowStore->get('products'));
    }

    public function testCreateTableAsSelectNoShadowReturnsSqlWhereFalse(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $plan = $rewriter->rewrite('CREATE TABLE archive AS SELECT id, name FROM users');
        self::assertSame(QueryKind::DDL_SIMULATED, $plan->kind());
        self::assertStringContainsString('"users" AS MATERIALIZED', $plan->sql());
    }

    public function testAlterTableThrowsUnsupported(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $this->expectException(UnsupportedSqlException::class);
        $rewriter->rewrite('ALTER TABLE users ADD COLUMN age INTEGER');
    }

    public function testRewriteMultipleWithDdl(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $multiPlan = $rewriter->rewriteMultiple("SELECT 1; CREATE TABLE t (id INTEGER); DROP TABLE users");
        self::assertSame(3, $multiPlan->count());
        self::assertSame(QueryKind::READ, $multiPlan->get(0)?->kind());
        self::assertSame(QueryKind::DDL_SIMULATED, $multiPlan->get(1)?->kind());
        self::assertSame(QueryKind::DDL_SIMULATED, $multiPlan->get(2)?->kind());
    }

    public function testSelectTableExistsInRegistryOnly(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $plan = $rewriter->rewrite('SELECT * FROM users WHERE id = 1');
        self::assertSame(QueryKind::READ, $plan->kind());
    }

    public function testSelectTableExistsInBothRegistryAndShadow(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $shadowStore->set('users', [['id' => 1, 'name' => 'Alice', 'email' => 'alice@ex.com']]);
        $plan = $rewriter->rewrite('SELECT * FROM users WHERE id = 1');
        self::assertSame(QueryKind::READ, $plan->kind());
        self::assertStringContainsString('"users" AS MATERIALIZED', $plan->sql());
    }

    public function testCommitIsSkipped(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $plan = $rewriter->rewrite('COMMIT');
        self::assertSame(QueryKind::SKIPPED, $plan->kind());
        self::assertSame('COMMIT', $plan->sql());
    }

    public function testRollbackIsSkipped(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $plan = $rewriter->rewrite('ROLLBACK');
        self::assertSame(QueryKind::SKIPPED, $plan->kind());
        self::assertSame('ROLLBACK', $plan->sql());
    }

    public function testRewriteWithLeadingWhitespaceTrimsFirst(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $plan = $rewriter->rewrite('  BEGIN  ');
        self::assertSame(QueryKind::SKIPPED, $plan->kind());
    }

    public function testRewriteMultipleWithLeadingWhitespace(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $multiPlan = $rewriter->rewriteMultiple('  SELECT 1  ');
        self::assertSame(1, $multiPlan->count());
        self::assertSame(QueryKind::READ, $multiPlan->get(0)?->kind());
    }

    public function testTruncateMutationTableName(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $plan = $rewriter->rewrite('TRUNCATE TABLE users');
        self::assertNotNull($plan->mutation());
        self::assertInstanceOf(TruncateMutation::class, $plan->mutation());
        self::assertSame('users', $plan->mutation()->tableName());
    }

    public function testInsertMutationTableName(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $plan = $rewriter->rewrite("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'a@b.com')");
        self::assertNotNull($plan->mutation());
        self::assertInstanceOf(InsertMutation::class, $plan->mutation());
        self::assertSame('users', $plan->mutation()->tableName());
    }

    public function testUpdateMutationTableName(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $plan = $rewriter->rewrite("UPDATE users SET name = 'Bob' WHERE id = 1");
        self::assertNotNull($plan->mutation());
        self::assertInstanceOf(UpdateMutation::class, $plan->mutation());
        self::assertSame('users', $plan->mutation()->tableName());
    }

    public function testDeleteMutationTableName(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $plan = $rewriter->rewrite('DELETE FROM users WHERE id = 1');
        self::assertNotNull($plan->mutation());
        self::assertInstanceOf(DeleteMutation::class, $plan->mutation());
        self::assertSame('users', $plan->mutation()->tableName());
    }

    public function testRewriteWithOnlyWhitespaceThrowsAfterTrim(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $this->expectException(UnsupportedSqlException::class);
        $rewriter->rewrite("\t \n");
    }

    public function testRewriteMultipleWithOnlyWhitespaceThrowsAfterTrim(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $this->expectException(UnsupportedSqlException::class);
        $rewriter->rewriteMultiple("\t \n");
    }

    public function testRewriteLeadingWhitespaceTrimsBeforeClassify(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $plan = $rewriter->rewrite("\n  SELECT * FROM users\n  ");
        self::assertSame(QueryKind::READ, $plan->kind());
    }

    public function testRewriteMultipleLeadingWhitespaceTrimsBeforeClassify(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $multiPlan = $rewriter->rewriteMultiple("\n  SELECT 1\n  ");
        self::assertSame(1, $multiPlan->count());
        self::assertSame(QueryKind::READ, $multiPlan->get(0)?->kind());
    }

    public function testDropTableThenCreateDoesNotThrow(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $plan = $rewriter->rewrite('DROP TABLE IF EXISTS users');
        self::assertSame(QueryKind::DDL_SIMULATED, $plan->kind());
        self::assertSame('SELECT 1 WHERE FALSE', $plan->sql());
        self::assertInstanceOf(DropTableMutation::class, $plan->mutation());
    }

    public function testInsertDoesNotCallEnsureDmlTarget(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('products', new TableDefinition(
            ['id', 'name'],
            ['id' => 'INTEGER', 'name' => 'TEXT'],
            ['id'],
            ['id'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);

        $rewriter->rewrite("INSERT INTO products (id, name) VALUES (1, 'Test')");
        self::assertSame([], $shadowStore->get('products'));
    }

    public function testUpdateEnsuresDmlTargetIsCalledForUpdate(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('items', new TableDefinition(
            ['id', 'qty'],
            ['id' => 'INTEGER', 'qty' => 'INTEGER'],
            ['id'],
            ['id'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'qty' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);

        $rewriter->rewrite('UPDATE items SET qty = 5 WHERE id = 1');
        self::assertSame([], $shadowStore->get('items'));
    }

    public function testDeleteEnsuresDmlTargetIsCalledForDelete(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('items', new TableDefinition(
            ['id', 'qty'],
            ['id' => 'INTEGER', 'qty' => 'INTEGER'],
            ['id'],
            ['id'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'qty' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);

        $rewriter->rewrite('DELETE FROM items WHERE id = 1');
        self::assertSame([], $shadowStore->get('items'));
    }

    public function testBuildTableContextShadowOnlyDerivesDifferentColumnsAcrossRows(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $shadowStore->set('misc', [
            ['a' => 1],
            ['a' => 2, 'b' => 3],
            ['a' => 4, 'c' => 5],
        ]);

        $plan = $rewriter->rewrite('SELECT * FROM misc');
        $sql = $plan->sql();
        self::assertStringContainsString('"misc" AS MATERIALIZED', $sql);
        self::assertStringContainsString('"a"', $sql);
        self::assertStringContainsString('"b"', $sql);
        self::assertStringContainsString('"c"', $sql);
    }

    public function testBuildTableContextRegistryAndShadowBothIncluded(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('table_a', new TableDefinition(
            ['id'],
            ['id' => 'INTEGER'],
            ['id'],
            ['id'],
            [],
            ['id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER')],
        ));
        $registry->register('table_b', new TableDefinition(
            ['val'],
            ['val' => 'TEXT'],
            [],
            [],
            [],
            ['val' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT')],
        ));
        $shadowStore->set('table_a', [['id' => 1]]);

        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);

        $plan = $rewriter->rewrite('SELECT * FROM table_a JOIN table_b ON TRUE');
        $sql = $plan->sql();
        self::assertStringContainsString('"table_a" AS MATERIALIZED', $sql);
        self::assertStringContainsString('"table_b" AS MATERIALIZED', $sql);
    }

    public function testSelectWithShadowOnlyHasSchemaContext(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);

        $shadowStore->set('my_table', [['x' => 1]]);

        $plan = $rewriter->rewrite('SELECT * FROM my_table');
        self::assertSame(QueryKind::READ, $plan->kind());
        self::assertStringContainsString('"my_table" AS MATERIALIZED', $plan->sql());
    }

    public function testSelectUnknownTableWithShadowContextThrows(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);

        $shadowStore->set('known_table', [['x' => 1]]);

        $this->expectException(\ZtdQuery\Exception\UnknownSchemaException::class);
        $rewriter->rewrite('SELECT * FROM unknown_table');
    }

    public function testCreateTableWithoutAsSelectReturnsSqlWhereFalse(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $plan = $rewriter->rewrite('CREATE TABLE orders (id INTEGER, amount NUMERIC)');
        self::assertSame('SELECT 1 WHERE FALSE', $plan->sql());
    }

    public function testCreateTableAsSelectTransformsSelectPart(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $plan = $rewriter->rewrite('CREATE TABLE archive AS SELECT * FROM users');
        self::assertSame(QueryKind::DDL_SIMULATED, $plan->kind());
        self::assertStringContainsString('users', $plan->sql());
    }

    public function testRewriteMultipleEmptyStringThrows(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $this->expectException(UnsupportedSqlException::class);
        $rewriter->rewriteMultiple('');
    }

    public function testRewriteMultipleSingleStatement(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $plans = $rewriter->rewriteMultiple('SELECT 1');
        self::assertCount(1, $plans->plans());
    }

    public function testRewriteMultipleMultiStatement(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $plans = $rewriter->rewriteMultiple('SELECT 1; SELECT 2');
        self::assertCount(2, $plans->plans());
    }

    public function testBuildTableContextShadowOnlyDifferentColumnKeysDetailed(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name'],
            ['id' => 'INTEGER', 'name' => 'TEXT'],
            ['id'],
            ['id'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);

        $rewriter->rewrite('DELETE FROM users WHERE id = 1');
        self::assertSame([], $shadowStore->get('users'));
    }

    public function testBuildTableContextShadowOnlyDifferentColumnKeys(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);

        $shadowStore->set('my_table', [
            ['id' => 1, 'name' => 'Alice'],
            ['id' => 2, 'name' => 'Bob', 'extra' => 'val'],
        ]);

        $plan = $rewriter->rewrite('SELECT * FROM my_table');
        self::assertStringContainsString('"my_table" AS MATERIALIZED', $plan->sql());
        self::assertStringContainsString('"extra"', $plan->sql());
    }

    public function testTruncateReturnsSelectWhereFalse(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $plan = $rewriter->rewrite('TRUNCATE TABLE users');
        self::assertSame('SELECT 1 WHERE FALSE', $plan->sql());
        self::assertSame(QueryKind::WRITE_SIMULATED, $plan->kind());
        self::assertInstanceOf(TruncateMutation::class, $plan->mutation());
    }

    public function testCreateTableAsSelectWithRegisteredSourceTable(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $plan = $rewriter->rewrite('CREATE TABLE archive AS SELECT id, name FROM users');
        self::assertSame(QueryKind::DDL_SIMULATED, $plan->kind());
        self::assertStringContainsString('"users" AS MATERIALIZED', $plan->sql());
    }

    public function testCreateTableNotAsSelectReturnsSqlWhereFalse(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $plan = $rewriter->rewrite('CREATE TABLE new_table (id INTEGER, name TEXT)');
        self::assertSame('SELECT 1 WHERE FALSE', $plan->sql());
        self::assertSame(QueryKind::DDL_SIMULATED, $plan->kind());
        self::assertInstanceOf(CreateTableMutation::class, $plan->mutation());
    }

    public function testRewriteInsertReturnsWriteSimulated(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $plan = $rewriter->rewrite("INSERT INTO users (id, name) VALUES (1, 'Alice')");
        self::assertSame(QueryKind::WRITE_SIMULATED, $plan->kind());
        self::assertInstanceOf(InsertMutation::class, $plan->mutation());
    }

    public function testRewriteUpdateReturnsWriteSimulated(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $plan = $rewriter->rewrite("UPDATE users SET name = 'Bob' WHERE id = 1");
        self::assertSame(QueryKind::WRITE_SIMULATED, $plan->kind());
        self::assertInstanceOf(UpdateMutation::class, $plan->mutation());
    }

    public function testRewriteDeleteReturnsWriteSimulated(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $plan = $rewriter->rewrite('DELETE FROM users WHERE id = 1');
        self::assertSame(QueryKind::WRITE_SIMULATED, $plan->kind());
        self::assertInstanceOf(DeleteMutation::class, $plan->mutation());
    }

    public function testRewriteDropTableReturnsSelectWhereFalse(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $plan = $rewriter->rewrite('DROP TABLE IF EXISTS users');
        self::assertSame('SELECT 1 WHERE FALSE', $plan->sql());
        self::assertSame(QueryKind::DDL_SIMULATED, $plan->kind());
        self::assertInstanceOf(DropTableMutation::class, $plan->mutation());
    }

    public function testRewriteUpdateEnsuresShadowStoreEntry(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name'],
            ['id' => 'INTEGER', 'name' => 'TEXT'],
            ['id'],
            ['id'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);

        self::assertFalse(array_key_exists('users', $shadowStore->getAll()));
        $rewriter->rewrite("UPDATE users SET name = 'Bob' WHERE id = 1");
        self::assertTrue(array_key_exists('users', $shadowStore->getAll()));
    }

    public function testRewriteDeleteEnsuresShadowStoreEntry(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name'],
            ['id' => 'INTEGER', 'name' => 'TEXT'],
            ['id'],
            ['id'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);

        self::assertFalse(array_key_exists('users', $shadowStore->getAll()));
        $rewriter->rewrite('DELETE FROM users WHERE id = 1');
        self::assertTrue(array_key_exists('users', $shadowStore->getAll()));
    }

    public function testRewriteInsertDoesNotCallEnsureDmlTarget(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('items', new TableDefinition(
            ['id', 'name'],
            ['id' => 'INTEGER', 'name' => 'TEXT'],
            ['id'],
            ['id'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);

        $rewriter->rewrite("INSERT INTO items (id, name) VALUES (1, 'Test')");
        self::assertFalse(array_key_exists('items', $shadowStore->getAll()));
    }

    public function testRewriteWithLeadingTrailingWhitespaceProducesValidSql(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $plan = $rewriter->rewrite("  \t SELECT * FROM users \n ");
        self::assertSame(QueryKind::READ, $plan->kind());
    }

    public function testRewriteMultipleWithLeadingTrailingWhitespaceProducesValidSql(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $multiPlan = $rewriter->rewriteMultiple("  \t SELECT 1 \n ");
        self::assertSame(1, $multiPlan->count());
    }

    public function testRewriteCreateTableAsSelectNonCreateStatementDoesNotEnterCtasBranch(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name', 'email'],
            ['id' => 'INTEGER', 'name' => 'VARCHAR(255)', 'email' => 'TEXT'],
            ['id'],
            ['id', 'name'],
            [],
            [
                'id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER'),
                'name' => new ColumnType(ColumnTypeFamily::STRING, 'VARCHAR(255)'),
                'email' => new ColumnType(ColumnTypeFamily::TEXT, 'TEXT'),
            ],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);
        $plan = $rewriter->rewrite('DROP TABLE IF EXISTS users');
        self::assertSame('SELECT 1 WHERE FALSE', $plan->sql());
        self::assertSame(QueryKind::DDL_SIMULATED, $plan->kind());
    }

    public function testBuildTableContextShadowOnlyWithEmptyRowsNoCte(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $registry->register('empty_table', new TableDefinition(
            ['id'],
            ['id' => 'INTEGER'],
            ['id'],
            ['id'],
            [],
            ['id' => new ColumnType(ColumnTypeFamily::INTEGER, 'INTEGER')],
        ));
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);

        $plan = $rewriter->rewrite('SELECT * FROM empty_table');
        self::assertStringContainsString('"empty_table" AS MATERIALIZED', $plan->sql());
    }

    public function testBuildTableContextShadowOnlyWithRowsDeriveColumns(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $parser = new PgSqlParser();
        $guard = new PgSqlQueryGuard($parser);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $schemaParser = new PgSqlSchemaParser();
        $mutationResolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, $parser);
        $rewriter = new PgSqlRewriter($guard, $shadowStore, $registry, $transformer, $mutationResolver, $parser);

        $shadowStore->set('derived', [['x' => 1, 'y' => 2]]);
        $plan = $rewriter->rewrite('SELECT * FROM derived');
        self::assertStringContainsString('"derived" AS MATERIALIZED', $plan->sql());
        self::assertStringContainsString('"x"', $plan->sql());
        self::assertStringContainsString('"y"', $plan->sql());
    }
}
