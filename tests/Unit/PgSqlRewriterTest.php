<?php

declare(strict_types=1);

namespace Tests\Unit;

use Tests\Contract\RewriterContractTest;
use ZtdQuery\Exception\UnknownSchemaException;
use ZtdQuery\Platform\SchemaParser;
use ZtdQuery\Rewrite\SqlRewriter;
use ZtdQuery\Exception\UnsupportedSqlException;
use ZtdQuery\Platform\Postgres\PgSqlCastRenderer;
use ZtdQuery\Platform\Postgres\PgSqlIdentifierQuoter;
use ZtdQuery\Platform\Postgres\PgSqlMutationResolver;
use ZtdQuery\Platform\Postgres\PgSqlParser;
use ZtdQuery\Platform\Postgres\PgSqlQueryGuard;
use ZtdQuery\Platform\Postgres\PgSqlReturningProjectionParser;
use ZtdQuery\Platform\Postgres\PgSqlRewriter;
use ZtdQuery\Platform\Postgres\PgSqlSchemaParser;
use ZtdQuery\Platform\Postgres\PgSqlPartitionParser;
use ZtdQuery\Platform\Postgres\PgSqlTransformer;
use ZtdQuery\Platform\Postgres\PgSqlViewDefinitionParser;
use ZtdQuery\Platform\Postgres\Transformer\DeleteTransformer;
use ZtdQuery\Platform\Postgres\Transformer\InsertTransformer;
use ZtdQuery\Platform\Postgres\Transformer\MergeTransformer;
use ZtdQuery\Platform\Postgres\Transformer\SelectTransformer;
use ZtdQuery\Platform\Postgres\Transformer\UpdateTransformer;
use ZtdQuery\Rewrite\QueryKind;
use ZtdQuery\Rewrite\AffectedRowsMode;
use ZtdQuery\Schema\ColumnType;
use ZtdQuery\Schema\ColumnTypeFamily;
use ZtdQuery\Schema\TableDefinition;
use ZtdQuery\Schema\TableDefinitionRegistry;
use ZtdQuery\Schema\TablePartitionRelation;
use ZtdQuery\Schema\ViewDefinition;
use ZtdQuery\Schema\ViewDefinitionSet;
use ZtdQuery\Shadow\Mutation\CreateTableMutation;
use ZtdQuery\Shadow\Mutation\DeleteMutation;
use ZtdQuery\Shadow\Mutation\DropTableMutation;
use ZtdQuery\Shadow\Mutation\InsertMutation;
use ZtdQuery\Shadow\Mutation\SynchronizeMutation;
use ZtdQuery\Shadow\Mutation\TruncateMutation;
use ZtdQuery\Shadow\Mutation\UpdateMutation;
use ZtdQuery\Shadow\ShadowStore;
use ZtdQuery\Shadow\ShadowTableState;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;

#[CoversClass(PgSqlRewriter::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlColumnTypeMapper::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlForeignKeyDefinitionParser::class)]
#[UsesClass(PgSqlParser::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlUpsertExpressionParser::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlConflictTarget::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PostgreSqlLexicalMasker::class)]
#[UsesClass(PgSqlSchemaParser::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlSelectRelationParser::class)]
#[UsesClass(PgSqlPartitionParser::class)]
#[UsesClass(PgSqlQueryGuard::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlReadOnlyDiagnosticStatement::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlTransactionStatementParser::class)]
#[UsesClass(PgSqlReturningProjectionParser::class)]
#[UsesClass(PgSqlMutationResolver::class)]
#[UsesClass(PgSqlTransformer::class)]
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
#[UsesClass(PgSqlIdentifierQuoter::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlValueRenderer::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlCteShadowComposer::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlNativeUpsertProjector::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlViewDefinitionParser::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlViewShadowRenderer::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlGeneratedColumnProjector::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlPartitionPredicateRenderer::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlLexerProfile::class)]
final class PgSqlRewriterTest extends RewriterContractTest
{
    public function testMergeBuildsAtomicSynchronizationPlan(): void
    {
        $store = new ShadowStore();
        $store->set('users', [['id' => 1, 'name' => 'old', 'email' => null]]);
        $store->set('source', [['id' => 1, 'name' => 'updated'], ['id' => 2, 'name' => 'inserted']]);
        $registry = new TableDefinitionRegistry();
        $users = $this->createSchemaParser()->parse($this->usersCreateTableSql());
        $source = $this->createSchemaParser()->parse(
            'CREATE TABLE source (id INTEGER PRIMARY KEY, name TEXT NOT NULL)',
        );
        self::assertNotNull($users);
        self::assertNotNull($source);
        $registry->register('users', $users);
        $registry->register('source', $source);

        $plan = $this->createRewriter($store, $registry)->rewrite(
            'MERGE INTO users AS target USING source ON target.id = source.id '
            . 'WHEN MATCHED THEN UPDATE SET name = source.name '
            . 'WHEN NOT MATCHED THEN INSERT (id, name) VALUES (source.id, source.name)',
        );

        self::assertSame(QueryKind::WRITE_SIMULATED, $plan->kind());
        self::assertSame(AffectedRowsMode::Changed, $plan->affectedRowsMode());
        self::assertInstanceOf(SynchronizeMutation::class, $plan->mutation());
        self::assertStringContainsString('WHERE NOT (EXISTS', $plan->sql());
        self::assertStringContainsString('UNION ALL', $plan->sql());
    }

    public function testDoBlockPassesThroughWithoutInspectingBodyTables(): void
    {
        $sql = "DO \$body\$ BEGIN INSERT INTO physical_only VALUES (1); END \$body\$ LANGUAGE plpgsql";
        $plan = $this->createRewriter(new ShadowStore(), new TableDefinitionRegistry())->rewrite($sql);

        self::assertSame(QueryKind::READ, $plan->kind());
        self::assertSame($sql, $plan->sql());
        self::assertNull($plan->mutation());
    }

    public function testChildPartitionSelectUsesFilteredParentShadowSource(): void
    {
        $store = new ShadowStore();
        $store->set('logs', [
            ['id' => 1, 'log_date' => '2024-05-01'],
            ['id' => 2, 'log_date' => '2025-05-01'],
        ]);
        $registry = new TableDefinitionRegistry();
        $parent = (new PgSqlSchemaParser())->parse(
            'CREATE TABLE logs (id INTEGER, log_date DATE, PRIMARY KEY (id, log_date)) '
            . 'PARTITION BY RANGE (log_date)',
        );
        self::assertNotNull($parent);
        $registry->register('logs', $parent);
        $registry->register('logs_2024', $parent->withPartitionRelation(new TablePartitionRelation(
            'logs',
            "(log_date >= '2024-01-01'::date) AND (log_date < '2025-01-01'::date)",
        )));

        $sql = $this->createRewriter($store, $registry)->rewrite('SELECT id FROM logs_2024')->sql();

        self::assertStringContainsString('"logs" AS MATERIALIZED', $sql);
        self::assertStringContainsString(
            '"logs_2024" AS MATERIALIZED (SELECT * FROM "logs" WHERE '
            . "(log_date >= '2024-01-01'::date) AND (log_date < '2025-01-01'::date))",
            $sql,
        );
        self::assertStringEndsWith('SELECT id FROM logs_2024', $sql);
    }

    public function testChildPartitionWriteMutationTargetsRootParent(): void
    {
        $store = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $parent = (new PgSqlSchemaParser())->parse(
            'CREATE TABLE logs (id INTEGER, log_date DATE, PRIMARY KEY (id, log_date)) '
            . 'PARTITION BY RANGE (log_date)',
        );
        self::assertNotNull($parent);
        $registry->register('logs', $parent);
        $registry->register('logs_2024', $parent->withPartitionRelation(new TablePartitionRelation(
            'logs',
            "log_date >= '2024-01-01'::date AND log_date < '2025-01-01'::date",
        )));

        $plan = $this->createRewriter($store, $registry)->rewrite(
            "INSERT INTO logs_2024 VALUES (1, '2024-05-01')",
        );

        self::assertNotNull($plan->mutation());
        self::assertSame('logs', $plan->mutation()->tableName());
        self::assertStringContainsString("CAST('2024-05-01' AS DATE) AS \"log_date\"", $plan->sql());
    }

    public function testDefaultPartitionExcludesOnlySpecificSiblingsOfSameParent(): void
    {
        $store = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $definition = new TableDefinition(['region'], ['region' => 'TEXT'], [], [], []);
        $registry->register('accounts', $definition);
        $registry->register('accounts_east', $definition->withPartitionRelation(
            new TablePartitionRelation('accounts', "region = 'east'"),
        ));
        $registry->register('accounts_other', $definition->withPartitionRelation(
            new TablePartitionRelation('accounts', null),
        ));
        $registry->register('unrelated', $definition->withPartitionRelation(
            new TablePartitionRelation('another_parent', "region = 'west'"),
        ));

        $sql = $this->createRewriter($store, $registry)->rewrite('SELECT * FROM accounts_other')->sql();

        self::assertStringContainsString(
            "SELECT * FROM \"accounts\" WHERE COALESCE(NOT ((region = 'east')), TRUE)",
            $sql,
        );
        self::assertStringNotContainsString("region = 'west'", $sql);
    }

    public function testNestedPartitionInsertAllocatesIdentityFromRootRows(): void
    {
        $store = new ShadowStore();
        $store->set('root_table', [['id' => 5, 'region' => 'east']]);
        $registry = new TableDefinitionRegistry();
        $definition = new TableDefinition(
            ['id', 'region'],
            ['id' => 'INTEGER', 'region' => 'TEXT'],
            ['id'],
            ['id'],
            [],
            identityStrategies: ['id' => \ZtdQuery\Schema\IdentityGenerationStrategy::MaxValue],
        );
        $registry->register('root_table', $definition);
        $registry->register('child_table', $definition->withPartitionRelation(
            new TablePartitionRelation('root_table', "region = 'east'"),
        ));
        $registry->register('grandchild_table', $definition->withPartitionRelation(
            new TablePartitionRelation('child_table', 'id < 10'),
        ));

        $sql = $this->createRewriter($store, $registry)->rewrite(
            "INSERT INTO grandchild_table (region) VALUES ('east')",
        )->sql();

        self::assertStringContainsString('6 AS "id"', $sql);
    }


    public function testGeneratedExpressionIsPresentBeforeTheFirstShadowWrite(): void
    {
        $store = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $definition = $this->createSchemaParser()->parse(
            'CREATE TABLE orders (qty INTEGER, total INTEGER GENERATED ALWAYS AS (qty * 2) STORED)',
        );
        self::assertNotNull($definition);
        $registry->register('orders', $definition);

        $sql = $this->createRewriter($store, $registry)->rewrite('SELECT total FROM orders')->sql();

        self::assertStringContainsString('(qty * 2) AS "total"', $sql);
    }

    public function testRegisteredViewIsKnownAndMaterialized(): void
    {
        $store = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $parser = new PgSqlParser();
        $schemaParser = new PgSqlSchemaParser();
        $definition = $schemaParser->parse($this->usersCreateTableSql());
        self::assertNotNull($definition);
        $registry->register('users', $definition);
        $store->set('users', [['id' => 1, 'name' => 'Alice', 'email' => 'alice@example.com']]);
        $selectTransformer = new SelectTransformer();
        $insertTransformer = new InsertTransformer($parser, $selectTransformer);
        $updateTransformer = new UpdateTransformer($parser, $selectTransformer);
        $deleteTransformer = new DeleteTransformer($parser, $selectTransformer);
        $transformer = new PgSqlTransformer($parser, $selectTransformer, $insertTransformer, $updateTransformer, $deleteTransformer);
        $resolver = new PgSqlMutationResolver($store, $registry, $schemaParser, $parser);
        $views = new ViewDefinitionSet();
        $views->register('active_users', (new PgSqlViewDefinitionParser())->fromQuery('SELECT id FROM public.users'));
        $rewriter = new PgSqlRewriter(new PgSqlQueryGuard($parser), $store, $registry, $transformer, $resolver, $parser, $views);

        $sql = $rewriter->rewrite('SELECT * FROM active_users')->sql();
        self::assertStringStartsWith('WITH "users" AS MATERIALIZED', $sql);
        self::assertStringContainsString('"active_users" AS MATERIALIZED (SELECT id FROM users)', $sql);

        $viewOnlyStore = new ShadowStore();
        $viewOnlyRegistry = new TableDefinitionRegistry();
        $viewOnlyViews = new ViewDefinitionSet();
        $viewOnlyViews->register('constant_view', (new PgSqlViewDefinitionParser())->fromQuery('SELECT 1 AS id'));
        $viewOnlyResolver = new PgSqlMutationResolver($viewOnlyStore, $viewOnlyRegistry, $schemaParser, $parser);
        $viewOnlyRewriter = new PgSqlRewriter(new PgSqlQueryGuard($parser), $viewOnlyStore, $viewOnlyRegistry, $transformer, $viewOnlyResolver, $parser, $viewOnlyViews);

        $this->expectException(UnknownSchemaException::class);
        $viewOnlyRewriter->rewrite('SELECT * FROM missing_table');
    }

    public function testRegisteredTableUpsertProjectsConflictExpression(): void
    {
        $store = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $definition = $this->createSchemaParser()->parse($this->usersCreateTableSql());
        self::assertNotNull($definition);
        $registry->register('users', $definition);

        $plan = $this->createRewriter($store, $registry)->rewrite(
            "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'a@example.com') "
            . 'ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name',
        );

        self::assertStringContainsString('__ztd_upsert_value_0', $plan->sql());
        self::assertStringNotContainsString('EXCLUDED.', $plan->sql());
    }

    public function testStoredTableUpsertProjectsConflictExpression(): void
    {
        $store = new ShadowStore();
        $store->ensure('users');
        $registry = new TableDefinitionRegistry();
        $definition = $this->createSchemaParser()->parse($this->usersCreateTableSql());
        self::assertNotNull($definition);
        $registry->register('users', $definition);

        $plan = $this->createRewriter($store, $registry)->rewrite(
            "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'a@example.com') "
            . 'ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name',
        );

        self::assertStringContainsString('__ztd_upsert_value_0', $plan->sql());
        self::assertStringNotContainsString('EXCLUDED.', $plan->sql());
    }

    public function testSchemaQualifiedSelectUsesShadowCte(): void
    {
        $store = new ShadowStore();
        $store->set('users', [['id' => 1, 'name' => 'Alice', 'email' => 'alice@example.com']]);
        $registry = new TableDefinitionRegistry();
        $definition = $this->createSchemaParser()->parse($this->usersCreateTableSql());
        self::assertNotNull($definition);
        $registry->register('users', $definition);

        $plan = $this->createRewriter($store, $registry)->rewrite('SELECT name FROM public.users');

        self::assertStringStartsWith('WITH "users" AS MATERIALIZED', $plan->sql());
        self::assertStringEndsWith('SELECT name FROM users', $plan->sql());
    }

    public function testExplainPassesThroughUnchanged(): void
    {
        $rewriter = $this->createRewriter(new ShadowStore(), new TableDefinitionRegistry());
        $sql = 'EXPLAIN (FORMAT JSON) SELECT * FROM users';

        $plan = $rewriter->rewrite($sql);

        self::assertSame(QueryKind::READ, $plan->kind());
        self::assertSame($sql, $plan->sql());
    }

    public function testShowPassesThroughUnchanged(): void
    {
        $rewriter = $this->createRewriter(new ShadowStore(), new TableDefinitionRegistry());
        $sql = 'SHOW server_version';

        $plan = $rewriter->rewrite($sql);

        self::assertSame(QueryKind::READ, $plan->kind());
        self::assertSame($sql, $plan->sql());
    }

    public function testTableFunctionDoesNotHideJoinedShadowRelation(): void
    {
        $store = new ShadowStore();
        $store->set('users', [['id' => 1, 'name' => 'Alice', 'email' => 'alice@example.com']]);
        $registry = new TableDefinitionRegistry();
        $definition = $this->createSchemaParser()->parse($this->usersCreateTableSql());
        self::assertNotNull($definition);
        $registry->register('users', $definition);

        $plan = $this->createRewriter($store, $registry)->rewrite('SELECT day.value, users.name FROM generate_series(1, 3) AS day(value) LEFT JOIN users ON users.id = day.value');

        self::assertStringStartsWith('WITH "users" AS MATERIALIZED', $plan->sql());
        self::assertStringContainsString('generate_series(1, 3)', $plan->sql());
    }

    public function testLateralDerivedTableKeepsNestedPhysicalRelationInShadowScope(): void
    {
        $store = new ShadowStore();
        $store->set('users', [['id' => 1, 'name' => 'Alice', 'email' => 'alice@example.com']]);
        $registry = new TableDefinitionRegistry();
        $definition = $this->createSchemaParser()->parse($this->usersCreateTableSql());
        self::assertNotNull($definition);
        $registry->register('users', $definition);

        $plan = $this->createRewriter($store, $registry)->rewrite('SELECT selected.id FROM LATERAL (SELECT id FROM users) AS selected');

        self::assertStringStartsWith('WITH "users" AS MATERIALIZED', $plan->sql());
    }

    public function testCteReferencesAreMatchedCaseInsensitivelyDuringSchemaValidation(): void
    {
        $registry = new TableDefinitionRegistry();
        $definition = $this->createSchemaParser()->parse('CREATE TABLE known_table (id INTEGER PRIMARY KEY)');
        self::assertNotNull($definition);
        $registry->register('known_table', $definition);

        $plan = $this->createRewriter(new ShadowStore(), $registry)->rewrite(
            'WITH users AS (SELECT 1 AS id) SELECT * FROM Users',
        );

        self::assertSame(QueryKind::READ, $plan->kind());
    }

    public function testUnknownTableAfterDeclaredCteIsRejected(): void
    {
        $registry = new TableDefinitionRegistry();
        $definition = $this->createSchemaParser()->parse('CREATE TABLE known_table (id INTEGER PRIMARY KEY)');
        self::assertNotNull($definition);
        $registry->register('known_table', $definition);

        $this->expectException(UnknownSchemaException::class);
        $this->expectExceptionMessage('missing_table');

        $this->createRewriter(new ShadowStore(), $registry)->rewrite(
            'WITH users AS (SELECT 1 AS id) SELECT * FROM Users JOIN missing_table ON TRUE',
        );
    }

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
        self::assertStringContainsString('"users"."id" AS "__ztd_original_id"', $plan->sql());
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

    public function testRewriteInsertUsesDefaultsFromRegistryWithoutShadowRows(): void
    {
        $definition = (new PgSqlSchemaParser())->parse("CREATE TABLE settings (id INTEGER, label TEXT DEFAULT 'new')");
        self::assertNotNull($definition);
        $registry = new TableDefinitionRegistry();
        $registry->register('settings', $definition);
        $rewriter = $this->createRewriter(new ShadowStore(), $registry);

        $plan = $rewriter->rewrite('INSERT INTO settings (id) VALUES (1)');

        self::assertStringContainsString("'new'", $plan->sql());
        self::assertStringContainsString('AS "label"', $plan->sql());
    }

    public function testRewriteInsertUsesIdentityStrategyFromRegistryWithoutShadowRows(): void
    {
        $definition = (new PgSqlSchemaParser())->parse('CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT)');
        self::assertNotNull($definition);
        $registry = new TableDefinitionRegistry();
        $registry->register('users', $definition);
        $rewriter = $this->createRewriter(new ShadowStore(), $registry);

        $plan = $rewriter->rewrite("INSERT INTO users (name) VALUES ('Alice')");

        self::assertStringContainsString('CAST(1 AS INTEGER) AS "id"', $plan->sql());
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

    public function testSelectRewritesTableAfterBlockComment(): void
    {
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(['id'], ['id' => 'INTEGER'], ['id'], [], []));
        $store = new ShadowStore();
        $store->set('users', [['id' => 1]]);
        $rewriter = $this->createRewriter($store, $registry);

        $plan = $rewriter->rewrite('SELECT * FROM/* table */users');

        self::assertSame(QueryKind::READ, $plan->kind());
        self::assertStringContainsString('"users" AS MATERIALIZED', $plan->sql());
    }

    public function testSelectIgnoresSqlKeywordsInsideLeadingLineComment(): void
    {
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(['id'], ['id' => 'INTEGER'], ['id'], [], []));
        $store = new ShadowStore();
        $store->set('users', [['id' => 1]]);
        $rewriter = $this->createRewriter($store, $registry);
        $sql = "-- SELECT * FROM other_table WHERE DELETE UPDATE INSERT\nSELECT * FROM users";

        $plan = $rewriter->rewrite($sql);

        self::assertSame(QueryKind::READ, $plan->kind());
        self::assertStringContainsString('"users" AS MATERIALIZED', $plan->sql());
        self::assertStringNotContainsString('"other_table" AS MATERIALIZED', $plan->sql());
    }

    public function testInsertResolvesTargetAfterBlockComment(): void
    {
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(['id'], ['id' => 'INTEGER'], ['id'], [], []));
        $rewriter = $this->createRewriter(new ShadowStore(), $registry);

        $plan = $rewriter->rewrite('INSERT INTO/* table */users (id) VALUES (1)');

        self::assertSame(QueryKind::WRITE_SIMULATED, $plan->kind());
        self::assertInstanceOf(InsertMutation::class, $plan->mutation());
        self::assertSame('users', $plan->mutation()->tableName());
    }

    public function testUpdateResolvesTargetAfterBlockComment(): void
    {
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(['id'], ['id' => 'INTEGER'], ['id'], [], []));
        $rewriter = $this->createRewriter(new ShadowStore(), $registry);

        $plan = $rewriter->rewrite('UPDATE/* table */users SET id = 2 WHERE id = 1');

        self::assertSame(QueryKind::WRITE_SIMULATED, $plan->kind());
        self::assertInstanceOf(UpdateMutation::class, $plan->mutation());
        self::assertSame('users', $plan->mutation()->tableName());
    }

    public function testDeleteResolvesTargetAfterBlockComment(): void
    {
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(['id'], ['id' => 'INTEGER'], ['id'], [], []));
        $rewriter = $this->createRewriter(new ShadowStore(), $registry);

        $plan = $rewriter->rewrite('DELETE FROM/* table */users WHERE id = 1');

        self::assertSame(QueryKind::WRITE_SIMULATED, $plan->kind());
        self::assertInstanceOf(DeleteMutation::class, $plan->mutation());
        self::assertSame('users', $plan->mutation()->tableName());
    }

    public function testUpdatePreservesDoubledQuoteStringBeforeWhere(): void
    {
        $registry = new TableDefinitionRegistry();
        $registry->register('notes', new TableDefinition(['id', 'body'], ['id' => 'INTEGER', 'body' => 'TEXT'], ['id'], [], []));
        $rewriter = $this->createRewriter(new ShadowStore(), $registry);

        $plan = $rewriter->rewrite("UPDATE notes SET body = 'it''s updated' WHERE id = 1");

        self::assertStringContainsString("'it''s updated' AS \"body\"", $plan->sql());
        self::assertStringContainsString('WHERE id = 1', $plan->sql());
    }

    public function testUpdatePreservesDollarQuotedStringBeforeWhere(): void
    {
        $registry = new TableDefinitionRegistry();
        $registry->register('notes', new TableDefinition(['id', 'body'], ['id' => 'INTEGER', 'body' => 'TEXT'], ['id'], [], []));
        $rewriter = $this->createRewriter(new ShadowStore(), $registry);

        $plan = $rewriter->rewrite('UPDATE notes SET body = $$reference to notes table$$ WHERE id = 1');

        self::assertStringContainsString('$$reference to notes table$$ AS "body"', $plan->sql());
        self::assertStringContainsString('WHERE id = 1', $plan->sql());
    }

    public function testUpdatePreservesEscapeStringBeforeWhere(): void
    {
        $registry = new TableDefinitionRegistry();
        $registry->register('notes', new TableDefinition(['id', 'body'], ['id' => 'INTEGER', 'body' => 'TEXT'], ['id'], [], []));
        $rewriter = $this->createRewriter(new ShadowStore(), $registry);

        $plan = $rewriter->rewrite("UPDATE notes SET body = E'line1\\nline2' WHERE id = 1");

        self::assertStringContainsString("E'line1\\nline2' AS \"body\"", $plan->sql());
        self::assertStringContainsString('WHERE id = 1', $plan->sql());
    }

    public function testUpdateDoesNotPromoteMaterializedUnknownTable(): void
    {
        $store = new ShadowStore();
        $store->insert('late_table', [['id' => 1, 'name' => 'Alice']]);
        $rewriter = $this->createRewriter($store, new TableDefinitionRegistry());

        try {
            $rewriter->rewrite("UPDATE late_table SET name = 'Bob' WHERE id = 1");
            self::fail('Expected an unknown schema exception.');
        } catch (UnknownSchemaException) {
            self::assertSame(ShadowTableState::Materialized, $store->state('late_table'));
        }
    }
}
