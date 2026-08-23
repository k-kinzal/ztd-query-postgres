<?php

declare(strict_types=1);

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use ZtdQuery\Exception\UnknownSchemaException;
use ZtdQuery\Exception\UnsupportedSqlException;
use ZtdQuery\Platform\Postgres\PgSqlMutationResolver;
use ZtdQuery\Platform\Postgres\PgSqlMergeParser;
use ZtdQuery\Platform\Postgres\PgSqlParser;
use ZtdQuery\Platform\Postgres\PgSqlSchemaParser;
use ZtdQuery\Platform\Postgres\PgSqlPartitionParser;
use ZtdQuery\Rewrite\QueryKind;
use ZtdQuery\Schema\TableDefinition;
use ZtdQuery\Schema\TableDefinitionRegistry;
use ZtdQuery\Schema\PartialUniqueIndex;
use ZtdQuery\Shadow\Mutation\CreateTableAsSelectMutation;
use ZtdQuery\Shadow\Mutation\CreateTableLikeMutation;
use ZtdQuery\Shadow\Mutation\CreateTableMutation;
use ZtdQuery\Shadow\Mutation\DeleteMutation;
use ZtdQuery\Shadow\Mutation\DropTableMutation;
use ZtdQuery\Shadow\Mutation\InsertMutation;
use ZtdQuery\Shadow\Mutation\MultiTruncateMutation;
use ZtdQuery\Shadow\Mutation\SynchronizeMutation;
use ZtdQuery\Shadow\Mutation\TruncateMutation;
use ZtdQuery\Shadow\Mutation\UpdateMutation;
use ZtdQuery\Shadow\Mutation\UpsertMutation;
use ZtdQuery\Shadow\ShadowStore;
use ZtdQuery\Shadow\ShadowTableState;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;

#[CoversClass(PgSqlMutationResolver::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlColumnTypeMapper::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlForeignKeyDefinitionParser::class)]
#[UsesClass(PgSqlParser::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlUpsertExpressionParser::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlConflictTarget::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PostgreSqlLexicalMasker::class)]
#[UsesClass(PgSqlSchemaParser::class)]
#[UsesClass(PgSqlPartitionParser::class)]
#[UsesClass(PgSqlMergeParser::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlMergeStatement::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlMergeClause::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlMergeMatchKind::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlMergeActionKind::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlCteShadowComposer::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlLexerProfile::class)]
final class PgSqlMutationResolverTest extends TestCase
{
    public function testPartitionDdlInheritsParentSchemaAndChildDmlUsesParentStorage(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $schemaParser = new PgSqlSchemaParser();
        $parent = $schemaParser->parse(
            'CREATE TABLE logs (id INTEGER, log_date DATE, PRIMARY KEY (id, log_date)) '
            . 'PARTITION BY RANGE (log_date)',
        );
        self::assertNotNull($parent);
        $registry->register('logs', $parent);
        $resolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, new PgSqlParser());

        $create = $resolver->resolve(
            "CREATE TABLE logs_2024 PARTITION OF logs FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')",
            'CREATE_TABLE',
            QueryKind::DDL_SIMULATED,
        );
        self::assertNotNull($create);
        $create->apply($shadowStore, []);

        $child = $registry->get('logs_2024');
        self::assertNotNull($child);
        self::assertSame($parent->columns, $child->columns);
        self::assertSame('logs', $child->partitionRelation?->parentTable);

        $insert = $resolver->resolve(
            "INSERT INTO logs_2024 VALUES (1, '2024-06-01')",
            'INSERT',
            QueryKind::WRITE_SIMULATED,
        );
        self::assertNotNull($insert);
        self::assertSame('logs', $insert->tableName());
        $insert->apply($shadowStore, [['id' => 1, 'log_date' => '2024-06-01']]);

        self::assertSame([['id' => 1, 'log_date' => '2024-06-01']], $shadowStore->get('logs'));
        self::assertSame([], $shadowStore->get('logs_2024'));
    }

    public function testPartitionDdlRejectsUnknownParentAndUnsupportedHashBounds(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $schemaParser = new PgSqlSchemaParser();
        $resolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, new PgSqlParser());

        try {
            $resolver->resolve(
                'CREATE TABLE child PARTITION OF missing FOR VALUES IN (1)',
                'CREATE_TABLE',
                QueryKind::DDL_SIMULATED,
            );
            self::fail('Expected an unknown parent error.');
        } catch (UnknownSchemaException) {
            self::assertFalse($registry->has('child'));
        }

        $parent = $schemaParser->parse('CREATE TABLE values_table (id INTEGER) PARTITION BY HASH (id)');
        self::assertNotNull($parent);
        $registry->register('values_table', $parent);

        $this->expectException(UnsupportedSqlException::class);
        $resolver->resolve(
            'CREATE TABLE values_0 PARTITION OF values_table FOR VALUES WITH (MODULUS 4, REMAINDER 0)',
            'CREATE_TABLE',
            QueryKind::DDL_SIMULATED,
        );
    }

    public function testSubpartitionDdlStoresItsOwnPartitionKey(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $schemaParser = new PgSqlSchemaParser();
        $parent = $schemaParser->parse(
            'CREATE TABLE logs (id INTEGER, log_date DATE, level TEXT) PARTITION BY RANGE (log_date)',
        );
        self::assertNotNull($parent);
        $registry->register('logs', $parent);
        $resolver = new PgSqlMutationResolver($shadowStore, $registry, $schemaParser, new PgSqlParser());

        $mutation = $resolver->resolve(
            "CREATE TABLE logs_2024 PARTITION OF logs FOR VALUES FROM ('2024-01-01') TO ('2025-01-01') "
            . 'PARTITION BY LIST (level)',
            'CREATE_TABLE',
            QueryKind::DDL_SIMULATED,
        );
        self::assertNotNull($mutation);
        $mutation->apply($shadowStore, []);

        $key = $registry->get('logs_2024')?->partitionKey;
        self::assertNotNull($key);
        self::assertSame(\ZtdQuery\Schema\TablePartitionStrategy::List, $key->strategy);
        self::assertSame(['level'], $key->expressions);
    }

    public function testNestedPartitionDmlTargetsRootStorage(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $definition = new TableDefinition(['id'], ['id' => 'INTEGER'], ['id'], [], []);
        $registry->register('root_table', $definition);
        $registry->register('child_table', $definition->withPartitionRelation(
            new \ZtdQuery\Schema\TablePartitionRelation('root_table', 'id >= 0'),
        ));
        $registry->register('grandchild_table', $definition->withPartitionRelation(
            new \ZtdQuery\Schema\TablePartitionRelation('child_table', 'id < 10'),
        ));
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser(),
        );

        $mutation = $resolver->resolve(
            'INSERT INTO grandchild_table VALUES (1)',
            'INSERT',
            QueryKind::WRITE_SIMULATED,
        );

        self::assertNotNull($mutation);
        self::assertSame('root_table', $mutation->tableName());
    }


    public function testResolveInsertReturnsInsertMutation(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $registry->register('users', new TableDefinition(
            ['id', 'name'],
            ['id' => 'INTEGER', 'name' => 'TEXT'],
            ['id'],
            [],
            []
        ));
        $shadowStore->set('users', [['id' => 1, 'name' => 'Existing']]);
        $mutation = $resolver->resolve(
            "INSERT INTO users (id, name) VALUES (1, 'Alice')",
            'INSERT',
            QueryKind::WRITE_SIMULATED
        );

        self::assertInstanceOf(InsertMutation::class, $mutation);
        $mutation->apply($shadowStore, [['id' => 1, 'name' => 'Inserted']]);
        self::assertCount(2, $shadowStore->get('users'));
    }

    public function testResolveInsertWithOnConflictDoUpdateReturnsUpsertMutation(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $registry->register('users', new TableDefinition(
            ['id', 'name'],
            ['id' => 'INTEGER', 'name' => 'TEXT'],
            ['id'],
            [],
            []
        ));
        $mutation = $resolver->resolve(
            "INSERT INTO users (id, name) VALUES (1, 'Alice') ON CONFLICT (id) DO UPDATE SET name = upper(users.name)",
            'INSERT',
            QueryKind::WRITE_SIMULATED
        );

        self::assertInstanceOf(UpsertMutation::class, $mutation);
    }

    public function testResolveInsertWithOnConflictDoNothingReturnsInsertMutation(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $registry->register('users', new TableDefinition(
            ['id', 'name'],
            ['id' => 'INTEGER', 'name' => 'TEXT'],
            ['id'],
            [],
            []
        ));
        $shadowStore->set('users', [['id' => 1, 'name' => 'Existing']]);
        $mutation = $resolver->resolve(
            "INSERT INTO users (id, name) VALUES (1, 'Alice') ON CONFLICT DO NOTHING",
            'INSERT',
            QueryKind::WRITE_SIMULATED
        );

        self::assertInstanceOf(InsertMutation::class, $mutation);
        $mutation->apply($shadowStore, [['id' => 1, 'name' => 'Ignored']]);
        self::assertSame([['id' => 1, 'name' => 'Existing']], $shadowStore->get('users'));
    }

    public function testResolveOnConflictDoUpdateWithoutRegisteredSchema(): void
    {
        $shadowStore = new ShadowStore();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            new TableDefinitionRegistry(),
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );

        $mutation = $resolver->resolve(
            "INSERT INTO users (id, name) VALUES (1, 'Alice') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name",
            'INSERT',
            QueryKind::WRITE_SIMULATED
        );

        self::assertInstanceOf(UpsertMutation::class, $mutation);
    }

    public function testResolveOnConflictDoNothingWithoutRegisteredSchema(): void
    {
        $shadowStore = new ShadowStore();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            new TableDefinitionRegistry(),
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );

        $mutation = $resolver->resolve(
            "INSERT INTO users (id, name) VALUES (1, 'Alice') ON CONFLICT DO NOTHING",
            'INSERT',
            QueryKind::WRITE_SIMULATED
        );

        self::assertInstanceOf(InsertMutation::class, $mutation);
    }

    public function testResolveInsertWithoutTableThrows(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $this->expectException(UnsupportedSqlException::class);
        $resolver->resolve('INSERT INTO', 'INSERT', QueryKind::WRITE_SIMULATED);
    }

    public function testResolveUpdateReturnsUpdateMutation(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $registry->register('users', new TableDefinition(
            ['id', 'name'],
            ['id' => 'INTEGER', 'name' => 'TEXT'],
            ['id'],
            [],
            []
        ));
        $mutation = $resolver->resolve(
            "UPDATE users SET name = 'Bob' WHERE id = 1",
            'UPDATE',
            QueryKind::WRITE_SIMULATED
        );

        self::assertInstanceOf(UpdateMutation::class, $mutation);
    }

    public function testResolveUpdateEnsuresShadowStore(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $registry->register('users', new TableDefinition(
            ['id', 'name'],
            ['id' => 'INTEGER', 'name' => 'TEXT'],
            ['id'],
            [],
            []
        ));
        $resolver->resolve(
            "UPDATE users SET name = 'Bob' WHERE id = 1",
            'UPDATE',
            QueryKind::WRITE_SIMULATED
        );

        self::assertSame(ShadowTableState::Initialized, $shadowStore->state('users'));
    }

    public function testResolveUpdateWithoutTableThrows(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $this->expectException(UnsupportedSqlException::class);
        $resolver->resolve('UPDATE', 'UPDATE', QueryKind::WRITE_SIMULATED);
    }

    public function testResolveDeleteReturnsDeleteMutation(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $registry->register('users', new TableDefinition(
            ['id', 'name'],
            ['id' => 'INTEGER', 'name' => 'TEXT'],
            ['id'],
            [],
            []
        ));
        $mutation = $resolver->resolve(
            'DELETE FROM users WHERE id = 1',
            'DELETE',
            QueryKind::WRITE_SIMULATED
        );

        self::assertInstanceOf(DeleteMutation::class, $mutation);
    }

    public function testResolveDeleteWithUnknownTableAndNoShadowThrows(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $this->expectException(UnknownSchemaException::class);
        $resolver->resolve(
            'DELETE FROM unknown_table WHERE id = 1',
            'DELETE',
            QueryKind::WRITE_SIMULATED
        );
    }

    public function testResolveDeleteWithShadowRowsDoesNotThrow(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $shadowStore->set('temp_table', [['id' => 1, 'name' => 'test']]);
        $mutation = $resolver->resolve(
            'DELETE FROM temp_table WHERE id = 1',
            'DELETE',
            QueryKind::WRITE_SIMULATED
        );

        self::assertInstanceOf(DeleteMutation::class, $mutation);
    }

    public function testResolveTruncateReturnsTruncateMutation(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $mutation = $resolver->resolve(
            'TRUNCATE TABLE users',
            'TRUNCATE',
            QueryKind::DDL_SIMULATED
        );

        self::assertInstanceOf(TruncateMutation::class, $mutation);
    }

    public function testResolveMultiTableTruncateReturnsMultiTruncateMutation(): void
    {
        $resolver = new PgSqlMutationResolver(
            new ShadowStore(),
            new TableDefinitionRegistry(),
            new PgSqlSchemaParser(),
            new PgSqlParser(),
        );

        $mutation = $resolver->resolve(
            'TRUNCATE TABLE alpha, beta RESTART IDENTITY',
            'TRUNCATE',
            QueryKind::WRITE_SIMULATED,
        );

        self::assertInstanceOf(MultiTruncateMutation::class, $mutation);
        self::assertSame(['alpha', 'beta'], $mutation->tableNames());
    }

    public function testResolveTruncateWithoutTableThrows(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $this->expectException(UnsupportedSqlException::class);
        $resolver->resolve('TRUNCATE', 'TRUNCATE', QueryKind::DDL_SIMULATED);
    }

    public function testResolveCreateTableReturnsCreateTableMutation(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $sql = 'CREATE TABLE posts ("id" INTEGER PRIMARY KEY, "title" TEXT NOT NULL)';
        $mutation = $resolver->resolve($sql, 'CREATE_TABLE', QueryKind::DDL_SIMULATED);

        self::assertInstanceOf(CreateTableMutation::class, $mutation);
    }

    public function testResolveCreateTableIfNotExistsWhenTableExists(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $registry->register('users', new TableDefinition(
            ['id', 'name'],
            ['id' => 'INTEGER', 'name' => 'TEXT'],
            ['id'],
            [],
            []
        ));
        $sql = 'CREATE TABLE IF NOT EXISTS users ("id" INTEGER PRIMARY KEY, "name" TEXT)';
        $mutation = $resolver->resolve($sql, 'CREATE_TABLE', QueryKind::DDL_SIMULATED);

        self::assertInstanceOf(CreateTableMutation::class, $mutation);
    }

    public function testResolveCreateTableWhenAlreadyExistsThrows(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $registry->register('users', new TableDefinition(
            ['id', 'name'],
            ['id' => 'INTEGER', 'name' => 'TEXT'],
            ['id'],
            [],
            []
        ));
        $sql = 'CREATE TABLE users ("id" INTEGER PRIMARY KEY, "name" TEXT)';

        $this->expectException(UnsupportedSqlException::class);
        $this->expectExceptionMessage('Table already exists');
        $resolver->resolve($sql, 'CREATE_TABLE', QueryKind::DDL_SIMULATED);
    }

    public function testResolveCreateTableLikeReturnsCreateTableLikeMutation(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $registry->register('users', new TableDefinition(
            ['id', 'name'],
            ['id' => 'INTEGER', 'name' => 'TEXT'],
            ['id'],
            [],
            []
        ));
        $sql = 'CREATE TABLE users_copy (LIKE users)';
        $mutation = $resolver->resolve($sql, 'CREATE_TABLE', QueryKind::DDL_SIMULATED);

        self::assertInstanceOf(CreateTableLikeMutation::class, $mutation);
    }

    public function testResolveCreateTableLikeWithUnknownSourceThrows(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $sql = 'CREATE TABLE users_copy (LIKE unknown_table)';
        $this->expectException(UnknownSchemaException::class);
        $resolver->resolve($sql, 'CREATE_TABLE', QueryKind::DDL_SIMULATED);
    }

    public function testResolveCreateTableAsSelectReturnsCreateTableAsSelectMutation(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $sql = 'CREATE TABLE archive AS SELECT id, name FROM users';
        $mutation = $resolver->resolve($sql, 'CREATE_TABLE', QueryKind::DDL_SIMULATED);

        self::assertInstanceOf(CreateTableAsSelectMutation::class, $mutation);
    }

    public function testResolveDropTableReturnsDropTableMutation(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $registry->register('users', new TableDefinition(
            ['id', 'name'],
            ['id' => 'INTEGER', 'name' => 'TEXT'],
            ['id'],
            [],
            []
        ));
        $mutation = $resolver->resolve(
            'DROP TABLE users',
            'DROP_TABLE',
            QueryKind::DDL_SIMULATED
        );

        self::assertInstanceOf(DropTableMutation::class, $mutation);
    }

    public function testResolveDropTableIfExistsWithUnknownTable(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $mutation = $resolver->resolve(
            'DROP TABLE IF EXISTS unknown_table',
            'DROP_TABLE',
            QueryKind::DDL_SIMULATED
        );

        self::assertInstanceOf(DropTableMutation::class, $mutation);
    }

    public function testResolveDropTableWithUnknownTableThrows(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $this->expectException(UnknownSchemaException::class);
        $resolver->resolve(
            'DROP TABLE unknown_table',
            'DROP_TABLE',
            QueryKind::DDL_SIMULATED
        );
    }

    public function testResolveAlterTableThrowsUnsupported(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $registry->register('users', new TableDefinition(
            ['id', 'name'],
            ['id' => 'INTEGER', 'name' => 'TEXT'],
            ['id'],
            [],
            []
        ));
        $this->expectException(UnsupportedSqlException::class);
        $this->expectExceptionMessage('ALTER TABLE not yet supported');
        $resolver->resolve(
            'ALTER TABLE users ADD COLUMN email TEXT',
            'ALTER_TABLE',
            QueryKind::DDL_SIMULATED
        );
    }

    public function testResolveAlterTableWithUnknownTableThrows(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $this->expectException(UnknownSchemaException::class);
        $resolver->resolve(
            'ALTER TABLE unknown_table ADD COLUMN email TEXT',
            'ALTER_TABLE',
            QueryKind::DDL_SIMULATED
        );
    }

    public function testResolveUnknownStatementTypeReturnsNull(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $mutation = $resolver->resolve('SOME SQL', 'UNKNOWN', QueryKind::READ);
        self::assertNull($mutation);
    }

    public function testResolveSelectReturnsNull(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $mutation = $resolver->resolve('SELECT 1', 'SELECT', QueryKind::READ);
        self::assertNull($mutation);
    }

    public function testResolveInsertReturnsPrimaryKeysFromRegistry(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $registry->register('users', new TableDefinition(
            ['id', 'name'],
            ['id' => 'INTEGER', 'name' => 'TEXT'],
            ['id'],
            [],
            []
        ));
        $mutation = $resolver->resolve(
            "INSERT INTO users (id, name) VALUES (1, 'Alice')",
            'INSERT',
            QueryKind::WRITE_SIMULATED
        );

        self::assertInstanceOf(InsertMutation::class, $mutation);
        self::assertSame('users', $mutation->tableName());
    }

    public function testResolveInsertWithoutRegistryReturnsEmptyPrimaryKeys(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $mutation = $resolver->resolve(
            "INSERT INTO users (id, name) VALUES (1, 'Alice')",
            'INSERT',
            QueryKind::WRITE_SIMULATED
        );

        self::assertInstanceOf(InsertMutation::class, $mutation);
    }

    public function testResolveUpsertReturnsPrimaryKeysAndConflictColumns(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $registry->register('users', new TableDefinition(
            ['id', 'name'],
            ['id' => 'INTEGER', 'name' => 'TEXT'],
            ['id'],
            [],
            []
        ));
        $mutation = $resolver->resolve(
            "INSERT INTO users (id, name) VALUES (1, 'Alice') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name",
            'INSERT',
            QueryKind::WRITE_SIMULATED
        );

        self::assertInstanceOf(UpsertMutation::class, $mutation);
        self::assertSame('users', $mutation->tableName());
    }

    public function testResolveUpsertValuesFromConflictClause(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $registry->register('users', new TableDefinition(
            ['id', 'name'],
            ['id' => 'INTEGER', 'name' => 'TEXT'],
            ['id'],
            [],
            []
        ));
        $mutation = $resolver->resolve(
            "INSERT INTO users (id, name) VALUES (1, 'Alice') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name",
            'INSERT',
            QueryKind::WRITE_SIMULATED
        );

        self::assertInstanceOf(UpsertMutation::class, $mutation);
    }

    public function testResolveUpdateReturnsPrimaryKeysFromRegistry(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $registry->register('users', new TableDefinition(
            ['id', 'name'],
            ['id' => 'INTEGER', 'name' => 'TEXT'],
            ['id'],
            [],
            []
        ));
        $mutation = $resolver->resolve(
            "UPDATE users SET name = 'Bob' WHERE id = 1",
            'UPDATE',
            QueryKind::WRITE_SIMULATED
        );

        self::assertInstanceOf(UpdateMutation::class, $mutation);
        self::assertSame('users', $mutation->tableName());
    }

    public function testResolveUpdateWithoutRegistryReturnsEmptyPrimaryKeys(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $shadowStore->set('users', [['id' => 1]]);
        $mutation = $resolver->resolve(
            "UPDATE users SET name = 'Bob' WHERE id = 1",
            'UPDATE',
            QueryKind::WRITE_SIMULATED
        );

        self::assertInstanceOf(UpdateMutation::class, $mutation);
    }

    public function testResolveDeleteReturnsPrimaryKeysFromRegistry(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $registry->register('users', new TableDefinition(
            ['id', 'name'],
            ['id' => 'INTEGER', 'name' => 'TEXT'],
            ['id'],
            [],
            []
        ));
        $mutation = $resolver->resolve(
            'DELETE FROM users WHERE id = 1',
            'DELETE',
            QueryKind::WRITE_SIMULATED
        );

        self::assertInstanceOf(DeleteMutation::class, $mutation);
        self::assertSame('users', $mutation->tableName());
    }

    public function testResolveDeleteWithoutRegistryReturnsEmptyPrimaryKeys(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $shadowStore->set('users', [['id' => 1]]);
        $mutation = $resolver->resolve(
            'DELETE FROM users WHERE id = 1',
            'DELETE',
            QueryKind::WRITE_SIMULATED
        );

        self::assertInstanceOf(DeleteMutation::class, $mutation);
    }

    public function testResolveDeleteEnsuresShadowStore(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $registry->register('users', new TableDefinition(
            ['id', 'name'],
            ['id' => 'INTEGER', 'name' => 'TEXT'],
            ['id'],
            [],
            []
        ));
        $resolver->resolve(
            'DELETE FROM users WHERE id = 1',
            'DELETE',
            QueryKind::WRITE_SIMULATED
        );

        self::assertSame([], $shadowStore->get('users'));
    }

    public function testResolveDeleteWithoutTableThrows(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $this->expectException(UnsupportedSqlException::class);
        $resolver->resolve('DELETE FROM', 'DELETE', QueryKind::WRITE_SIMULATED);
    }

    public function testResolveCreateTableWithoutTableNameThrows(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $this->expectException(UnsupportedSqlException::class);
        $resolver->resolve('CREATE TABLE', 'CREATE_TABLE', QueryKind::DDL_SIMULATED);
    }

    public function testResolveCreateTableLikeWithNullSourceThrows(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $this->expectException(UnknownSchemaException::class);
        $resolver->resolve('CREATE TABLE users_copy (LIKE )', 'CREATE_TABLE', QueryKind::DDL_SIMULATED);
    }

    public function testResolveCreateTableAsSelectExtractsColumns(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $sql = 'CREATE TABLE archive AS SELECT id, name FROM users';
        $mutation = $resolver->resolve($sql, 'CREATE_TABLE', QueryKind::DDL_SIMULATED);

        self::assertInstanceOf(CreateTableAsSelectMutation::class, $mutation);
        self::assertSame('archive', $mutation->tableName());
    }

    public function testResolveCreateTableAsSelectWithStar(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $sql = 'CREATE TABLE archive AS SELECT * FROM users';
        $mutation = $resolver->resolve($sql, 'CREATE_TABLE', QueryKind::DDL_SIMULATED);

        self::assertInstanceOf(CreateTableAsSelectMutation::class, $mutation);
    }

    public function testResolveCreateTableAsSelectWithAliases(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $sql = 'CREATE TABLE archive AS SELECT id, name AS full_name FROM users';
        $mutation = $resolver->resolve($sql, 'CREATE_TABLE', QueryKind::DDL_SIMULATED);

        self::assertInstanceOf(CreateTableAsSelectMutation::class, $mutation);
    }

    public function testResolveCreateTableAsSelectWithExpressions(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $sql = 'CREATE TABLE archive AS SELECT UPPER(name) FROM users';
        $mutation = $resolver->resolve($sql, 'CREATE_TABLE', QueryKind::DDL_SIMULATED);

        self::assertInstanceOf(CreateTableAsSelectMutation::class, $mutation);
    }

    public function testResolveCreateTableAsSelectWithNoFrom(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $sql = 'CREATE TABLE archive AS SELECT 1 AS id, \'test\' AS name';
        $mutation = $resolver->resolve($sql, 'CREATE_TABLE', QueryKind::DDL_SIMULATED);

        self::assertInstanceOf(CreateTableAsSelectMutation::class, $mutation);
    }

    public function testResolveCreateTableAsSelectIfNotExists(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $sql = 'CREATE TABLE IF NOT EXISTS archive AS SELECT id, name FROM users';
        $mutation = $resolver->resolve($sql, 'CREATE_TABLE', QueryKind::DDL_SIMULATED);

        self::assertInstanceOf(CreateTableAsSelectMutation::class, $mutation);
    }

    public function testResolveDropTableWithoutTableNameThrows(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $this->expectException(UnsupportedSqlException::class);
        $resolver->resolve('DROP TABLE', 'DROP_TABLE', QueryKind::DDL_SIMULATED);
    }

    public function testResolveAlterTableWithoutTableNameThrows(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $this->expectException(UnsupportedSqlException::class);
        $resolver->resolve('ALTER TABLE', 'ALTER_TABLE', QueryKind::DDL_SIMULATED);
    }

    public function testResolveCreateTableAsSelectWithLowercaseSql(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $sql = 'create table archive as select id, name from users';
        $mutation = $resolver->resolve($sql, 'CREATE_TABLE', QueryKind::DDL_SIMULATED);

        self::assertInstanceOf(CreateTableAsSelectMutation::class, $mutation);
    }

    public function testResolveCreateTableAsSelectWithTableQualifiedColumns(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $sql = 'CREATE TABLE archive AS SELECT u.id, u.name FROM users u';
        $mutation = $resolver->resolve($sql, 'CREATE_TABLE', QueryKind::DDL_SIMULATED);

        self::assertInstanceOf(CreateTableAsSelectMutation::class, $mutation);
    }

    public function testResolveCreateTableAsSelectWithQuotedAlias(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $sql = 'CREATE TABLE archive AS SELECT id, name AS "full_name" FROM users';
        $mutation = $resolver->resolve($sql, 'CREATE_TABLE', QueryKind::DDL_SIMULATED);

        self::assertInstanceOf(CreateTableAsSelectMutation::class, $mutation);
    }

    public function testResolveInsertLowercaseExtractsTable(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $mutation = $resolver->resolve(
            "insert into users (id, name) values (1, 'Alice')",
            'INSERT',
            QueryKind::WRITE_SIMULATED
        );

        self::assertInstanceOf(InsertMutation::class, $mutation);
        self::assertSame('users', $mutation->tableName());
    }

    public function testResolveUpdateLowercaseExtractsTable(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $shadowStore->set('users', [['id' => 1]]);
        $mutation = $resolver->resolve(
            "update users set name = 'Bob' where id = 1",
            'UPDATE',
            QueryKind::WRITE_SIMULATED
        );

        self::assertInstanceOf(UpdateMutation::class, $mutation);
        self::assertSame('users', $mutation->tableName());
    }

    public function testResolveDeleteLowercaseExtractsTable(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $registry->register('users', new TableDefinition(
            ['id', 'name'],
            ['id' => 'INTEGER', 'name' => 'TEXT'],
            ['id'],
            [],
            []
        ));
        $mutation = $resolver->resolve(
            'delete from users where id = 1',
            'DELETE',
            QueryKind::WRITE_SIMULATED
        );

        self::assertInstanceOf(DeleteMutation::class, $mutation);
        self::assertSame('users', $mutation->tableName());
    }

    public function testResolveTruncateLowercaseExtractsTable(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $mutation = $resolver->resolve(
            'truncate table users',
            'TRUNCATE',
            QueryKind::DDL_SIMULATED
        );

        self::assertInstanceOf(TruncateMutation::class, $mutation);
        self::assertSame('users', $mutation->tableName());
    }

    public function testResolveCreateTableLowercaseExtractsTable(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $sql = 'create table posts ("id" integer primary key, "title" text not null)';
        $mutation = $resolver->resolve($sql, 'CREATE_TABLE', QueryKind::DDL_SIMULATED);

        self::assertInstanceOf(CreateTableMutation::class, $mutation);
    }

    public function testResolveDropTableLowercaseExtractsTable(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $registry->register('users', new TableDefinition(
            ['id', 'name'],
            ['id' => 'INTEGER', 'name' => 'TEXT'],
            ['id'],
            [],
            []
        ));
        $mutation = $resolver->resolve(
            'drop table users',
            'DROP_TABLE',
            QueryKind::DDL_SIMULATED
        );

        self::assertInstanceOf(DropTableMutation::class, $mutation);
    }

    public function testResolveCreateTableAsSelectWithCommaInSelect(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $sql = 'CREATE TABLE archive AS SELECT id, name, COALESCE(email, \'none\') AS email FROM users';
        $mutation = $resolver->resolve($sql, 'CREATE_TABLE', QueryKind::DDL_SIMULATED);

        self::assertInstanceOf(CreateTableAsSelectMutation::class, $mutation);
    }

    public function testResolveCreateTableAsSelectWithSubquery(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $sql = 'CREATE TABLE archive AS SELECT id, (SELECT COUNT(*) FROM orders WHERE orders.user_id = users.id) AS order_count FROM users';
        $mutation = $resolver->resolve($sql, 'CREATE_TABLE', QueryKind::DDL_SIMULATED);

        self::assertInstanceOf(CreateTableAsSelectMutation::class, $mutation);
    }

    public function testResolveCreateTableLikeWithIfNotExists(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $registry->register('users', new TableDefinition(
            ['id', 'name'],
            ['id' => 'INTEGER', 'name' => 'TEXT'],
            ['id'],
            [],
            []
        ));
        $sql = 'CREATE TABLE IF NOT EXISTS users_copy (LIKE users)';
        $mutation = $resolver->resolve($sql, 'CREATE_TABLE', QueryKind::DDL_SIMULATED);

        self::assertInstanceOf(CreateTableLikeMutation::class, $mutation);
    }

    public function testResolveDropTableIfExistsForRegisteredTable(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $registry->register('users', new TableDefinition(
            ['id', 'name'],
            ['id' => 'INTEGER', 'name' => 'TEXT'],
            ['id'],
            [],
            []
        ));
        $mutation = $resolver->resolve(
            'DROP TABLE IF EXISTS users',
            'DROP_TABLE',
            QueryKind::DDL_SIMULATED
        );

        self::assertInstanceOf(DropTableMutation::class, $mutation);
    }

    public function testResolveCreateTableIfNotExistsDoesNotThrowWhenAlreadyExists(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $registry->register('users', new TableDefinition(
            ['id', 'name'],
            ['id' => 'INTEGER', 'name' => 'TEXT'],
            ['id'],
            [],
            []
        ));
        $sql = 'CREATE TABLE IF NOT EXISTS users ("id" INTEGER PRIMARY KEY, "name" TEXT)';
        $mutation = $resolver->resolve($sql, 'CREATE_TABLE', QueryKind::DDL_SIMULATED);

        self::assertInstanceOf(CreateTableMutation::class, $mutation);
    }

    public function testResolveCreateTableAsSelectWithColumnsMultiline(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $sql = "CREATE TABLE archive AS\nSELECT\nid, name\nFROM users";
        $mutation = $resolver->resolve($sql, 'CREATE_TABLE', QueryKind::DDL_SIMULATED);

        self::assertInstanceOf(CreateTableAsSelectMutation::class, $mutation);
        self::assertSame('archive', $mutation->tableName());
    }

    public function testResolveCreateTableAsSelectWithColumnAlias(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $sql = 'CREATE TABLE archive AS SELECT id, name AS full_name FROM users';
        $mutation = $resolver->resolve($sql, 'CREATE_TABLE', QueryKind::DDL_SIMULATED);

        self::assertInstanceOf(CreateTableAsSelectMutation::class, $mutation);
    }

    public function testResolveCreateTableAsSelectWithExpression(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $sql = 'CREATE TABLE stats AS SELECT COUNT(*) FROM users';
        $mutation = $resolver->resolve($sql, 'CREATE_TABLE', QueryKind::DDL_SIMULATED);

        self::assertInstanceOf(CreateTableAsSelectMutation::class, $mutation);
    }

    public function testResolveCreateTableAsSelectWithWildcard(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $sql = 'CREATE TABLE archive AS SELECT * FROM users';
        $mutation = $resolver->resolve($sql, 'CREATE_TABLE', QueryKind::DDL_SIMULATED);

        self::assertInstanceOf(CreateTableAsSelectMutation::class, $mutation);
    }

    public function testResolveCreateTableAsSelectNoFrom(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $sql = 'CREATE TABLE constants AS SELECT 1 AS id, \'hello\' AS msg';
        $mutation = $resolver->resolve($sql, 'CREATE_TABLE', QueryKind::DDL_SIMULATED);

        self::assertInstanceOf(CreateTableAsSelectMutation::class, $mutation);
    }

    public function testResolveCreateTableAsSelectWithQuotedColumn(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $sql = 'CREATE TABLE archive AS SELECT "Id", "Name" FROM users';
        $mutation = $resolver->resolve($sql, 'CREATE_TABLE', QueryKind::DDL_SIMULATED);

        self::assertInstanceOf(CreateTableAsSelectMutation::class, $mutation);
        $mutation->apply($shadowStore, []);
        $definition = $registry->get('archive');
        self::assertNotNull($definition);
        self::assertSame(['Id', 'Name'], $definition->columns);
    }

    public function testResolveCreateTableAsSelectWithTableQualifiedColumn(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $sql = 'CREATE TABLE archive AS SELECT users.id, users.name FROM users';
        $mutation = $resolver->resolve($sql, 'CREATE_TABLE', QueryKind::DDL_SIMULATED);

        self::assertInstanceOf(CreateTableAsSelectMutation::class, $mutation);
    }

    public function testResolveCreateTableAsSelectCommaInSubquery(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $sql = 'CREATE TABLE archive AS SELECT (SELECT 1, 2) AS sub, name FROM users';
        $mutation = $resolver->resolve($sql, 'CREATE_TABLE', QueryKind::DDL_SIMULATED);

        self::assertInstanceOf(CreateTableAsSelectMutation::class, $mutation);
    }

    public function testResolveAlterTableUnknownTableThrows(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $this->expectException(UnknownSchemaException::class);
        $resolver->resolve('ALTER TABLE nonexistent ADD COLUMN x INT', 'ALTER_TABLE', QueryKind::DDL_SIMULATED);
    }

    public function testResolveDeleteWithShadowStoreData(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $registry->register('users', new TableDefinition(
            ['id', 'name'],
            ['id' => 'INTEGER', 'name' => 'TEXT'],
            ['id'],
            [],
            []
        ));
        $shadowStore->set('users', [['id' => 1, 'name' => 'Alice']]);
        $mutation = $resolver->resolve('DELETE FROM users WHERE id = 1', 'DELETE', QueryKind::WRITE_SIMULATED);

        self::assertInstanceOf(DeleteMutation::class, $mutation);
        self::assertSame('users', $mutation->tableName());
    }

    public function testResolveDeleteNoRegistryNoShadowThrows(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $this->expectException(UnknownSchemaException::class);
        $resolver->resolve('DELETE FROM unknown_table WHERE id = 1', 'DELETE', QueryKind::WRITE_SIMULATED);
    }

    public function testResolveInsertWithOnConflictDoNothing(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $registry->register('users', new TableDefinition(
            ['id', 'name'],
            ['id' => 'INTEGER', 'name' => 'TEXT'],
            ['id'],
            [],
            []
        ));
        $mutation = $resolver->resolve(
            "INSERT INTO users (id, name) VALUES (1, 'Alice') ON CONFLICT (id) DO NOTHING",
            'INSERT',
            QueryKind::WRITE_SIMULATED
        );

        self::assertInstanceOf(InsertMutation::class, $mutation);
    }

    public function testResolveInsertWithOnConflictDoUpdate(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $registry->register('users', new TableDefinition(
            ['id', 'name'],
            ['id' => 'INTEGER', 'name' => 'TEXT'],
            ['id'],
            [],
            []
        ));
        $mutation = $resolver->resolve(
            "INSERT INTO users (id, name) VALUES (1, 'Alice') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name",
            'INSERT',
            QueryKind::WRITE_SIMULATED
        );

        self::assertInstanceOf(UpsertMutation::class, $mutation);
        self::assertSame('users', $mutation->tableName());
    }

    public function testResolveInsertTableNotFoundThrows(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $this->expectException(UnsupportedSqlException::class);
        $resolver->resolve('INSERT INTO', 'INSERT', QueryKind::WRITE_SIMULATED);
    }

    public function testResolveUpdateTableNotFoundThrows(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $this->expectException(UnsupportedSqlException::class);
        $resolver->resolve('UPDATE', 'UPDATE', QueryKind::WRITE_SIMULATED);
    }

    public function testResolveDeleteTableNotFoundThrows(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $this->expectException(UnsupportedSqlException::class);
        $resolver->resolve('DELETE FROM', 'DELETE', QueryKind::WRITE_SIMULATED);
    }

    public function testResolveTruncateTableNotFoundThrows(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $this->expectException(UnsupportedSqlException::class);
        $resolver->resolve('TRUNCATE', 'TRUNCATE', QueryKind::WRITE_SIMULATED);
    }

    public function testResolveCreateTableNameNotFoundThrows(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $this->expectException(UnsupportedSqlException::class);
        $resolver->resolve('CREATE TABLE', 'CREATE_TABLE', QueryKind::DDL_SIMULATED);
    }

    public function testResolveDropTableNameNotFoundThrows(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $this->expectException(UnsupportedSqlException::class);
        $resolver->resolve('DROP TABLE', 'DROP_TABLE', QueryKind::DDL_SIMULATED);
    }

    public function testResolveAlterTableNameNotFoundThrows(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $this->expectException(UnsupportedSqlException::class);
        $resolver->resolve('ALTER TABLE', 'ALTER_TABLE', QueryKind::DDL_SIMULATED);
    }

    public function testResolveDropTableWithoutIfExistsNotRegisteredThrows(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $this->expectException(UnknownSchemaException::class);
        $resolver->resolve('DROP TABLE nonexistent', 'DROP_TABLE', QueryKind::DDL_SIMULATED);
    }

    public function testResolveDropTableWithIfExistsNotRegistered(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $mutation = $resolver->resolve('DROP TABLE IF EXISTS nonexistent', 'DROP_TABLE', QueryKind::DDL_SIMULATED);

        self::assertInstanceOf(DropTableMutation::class, $mutation);
    }

    public function testResolveCreateTableAlreadyExistsThrows(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $registry->register('users', new TableDefinition(
            ['id', 'name'],
            ['id' => 'INTEGER', 'name' => 'TEXT'],
            ['id'],
            [],
            []
        ));
        $this->expectException(UnsupportedSqlException::class);
        $resolver->resolve('CREATE TABLE users (id INTEGER)', 'CREATE_TABLE', QueryKind::DDL_SIMULATED);
    }

    public function testResolveCreateTableLikeSourceNotRegisteredThrows(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $this->expectException(UnknownSchemaException::class);
        $resolver->resolve('CREATE TABLE new_t (LIKE nonexistent)', 'CREATE_TABLE', QueryKind::DDL_SIMULATED);
    }

    public function testResolveCreateTableLikeSourceRegistered(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $registry->register('source_table', new TableDefinition(
            ['id', 'name'],
            ['id' => 'INTEGER', 'name' => 'TEXT'],
            ['id'],
            [],
            []
        ));
        $mutation = $resolver->resolve('CREATE TABLE new_t (LIKE source_table)', 'CREATE_TABLE', QueryKind::DDL_SIMULATED);

        self::assertInstanceOf(CreateTableLikeMutation::class, $mutation);
        self::assertSame('new_t', $mutation->tableName());
    }

    public function testResolveUpdateWithRegisteredTable(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $registry->register('users', new TableDefinition(
            ['id', 'name'],
            ['id' => 'INTEGER', 'name' => 'TEXT'],
            ['id'],
            [],
            []
        ));
        $mutation = $resolver->resolve("UPDATE users SET name = 'Bob' WHERE id = 1", 'UPDATE', QueryKind::WRITE_SIMULATED);

        self::assertInstanceOf(UpdateMutation::class, $mutation);
        self::assertSame('users', $mutation->tableName());
    }

    public function testResolveInsertWithoutOnConflict(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $registry->register('users', new TableDefinition(
            ['id', 'name'],
            ['id' => 'INTEGER', 'name' => 'TEXT'],
            ['id'],
            [],
            []
        ));
        $mutation = $resolver->resolve(
            "INSERT INTO users (id, name) VALUES (1, 'Alice')",
            'INSERT',
            QueryKind::WRITE_SIMULATED
        );

        self::assertInstanceOf(InsertMutation::class, $mutation);
        self::assertSame('users', $mutation->tableName());
    }

    public function testResolveInsertNoRegistryStillWorks(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $mutation = $resolver->resolve(
            "INSERT INTO new_table (id, name) VALUES (1, 'Alice')",
            'INSERT',
            QueryKind::WRITE_SIMULATED
        );

        self::assertInstanceOf(InsertMutation::class, $mutation);
        self::assertSame('new_table', $mutation->tableName());
    }

    public function testResolveUpdateNoRegistryThrowsUnknownSchema(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $this->expectException(UnknownSchemaException::class);
        $resolver->resolve(
            "UPDATE new_table SET name = 'Bob' WHERE id = 1",
            'UPDATE',
            QueryKind::WRITE_SIMULATED
        );
    }

    public function testResolveCreateTableAsSelectWithLowercaseSelectAndAs(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $sql = 'CREATE TABLE archive AS select id, name as full_name from users';
        $mutation = $resolver->resolve($sql, 'CREATE_TABLE', QueryKind::DDL_SIMULATED);

        self::assertInstanceOf(CreateTableAsSelectMutation::class, $mutation);
    }

    public function testResolveCreateTableAsSelectWithNewlineInSelectList(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $sql = "CREATE TABLE archive AS SELECT id,\nname FROM users";
        $mutation = $resolver->resolve($sql, 'CREATE_TABLE', QueryKind::DDL_SIMULATED);

        self::assertInstanceOf(CreateTableAsSelectMutation::class, $mutation);
    }

    public function testResolveCreateTableAsSelectNoFromLowercase(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $sql = "CREATE TABLE constants AS select 1 as id, 'hello' as msg";
        $mutation = $resolver->resolve($sql, 'CREATE_TABLE', QueryKind::DDL_SIMULATED);

        self::assertInstanceOf(CreateTableAsSelectMutation::class, $mutation);
    }

    public function testResolveUpsertConflictValuesPreserved(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $registry->register('users', new TableDefinition(
            ['id', 'name'],
            ['id' => 'INTEGER', 'name' => 'TEXT'],
            ['id'],
            [],
            []
        ));
        $shadowStore->set('users', [['id' => 1, 'name' => 'Existing']]);
        $mutation = $resolver->resolve(
            "INSERT INTO users (id, name) VALUES (1, 'Alice') ON CONFLICT (id) DO UPDATE SET name = 'Bob', id = EXCLUDED.id",
            'INSERT',
            QueryKind::WRITE_SIMULATED
        );

        self::assertInstanceOf(UpsertMutation::class, $mutation);
        $mutation->apply($shadowStore, [['id' => 1, 'name' => 'Alice']]);
        self::assertSame([['id' => 1, 'name' => 'Bob']], $shadowStore->get('users'));
    }

    public function testResolveDeleteWithShadowStoreOnlyNoRegistry(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $shadowStore->set('temp', [['id' => 1]]);
        $mutation = $resolver->resolve(
            'DELETE FROM temp WHERE id = 1',
            'DELETE',
            QueryKind::WRITE_SIMULATED
        );

        self::assertInstanceOf(DeleteMutation::class, $mutation);
        self::assertSame('temp', $mutation->tableName());
    }

    public function testResolveUpdateEnsuresShadowStoreEntry(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $registry->register('users', new TableDefinition(
            ['id', 'name'],
            ['id' => 'INTEGER', 'name' => 'TEXT'],
            ['id'],
            [],
            []
        ));
        self::assertFalse(array_key_exists('users', $shadowStore->getAll()));
        $resolver->resolve(
            "UPDATE users SET name = 'Bob' WHERE id = 1",
            'UPDATE',
            QueryKind::WRITE_SIMULATED
        );
        self::assertTrue(array_key_exists('users', $shadowStore->getAll()));
    }

    public function testResolveDeleteEnsuresShadowStoreEntry(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $registry->register('users', new TableDefinition(
            ['id', 'name'],
            ['id' => 'INTEGER', 'name' => 'TEXT'],
            ['id'],
            [],
            []
        ));
        self::assertFalse(array_key_exists('users', $shadowStore->getAll()));
        $resolver->resolve(
            'DELETE FROM users WHERE id = 1',
            'DELETE',
            QueryKind::WRITE_SIMULATED
        );
        self::assertTrue(array_key_exists('users', $shadowStore->getAll()));
    }

    public function testResolveUpsertConflictValuesMapping(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $registry->register('users', new TableDefinition(
            ['id', 'name'],
            ['id' => 'INTEGER', 'name' => 'TEXT'],
            ['id'],
            [],
            []
        ));
        $mutation = $resolver->resolve(
            "INSERT INTO users (id, name) VALUES (1, 'Alice') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name",
            'INSERT',
            QueryKind::WRITE_SIMULATED
        );

        self::assertInstanceOf(UpsertMutation::class, $mutation);
        self::assertSame('users', $mutation->tableName());
    }

    public function testResolveUpsertWithRegisteredTableUsesRegistry(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $registry->register('users', new TableDefinition(
            ['id', 'name'],
            ['id' => 'INTEGER', 'name' => 'TEXT'],
            ['id'],
            [],
            []
        ));
        $mutation = $resolver->resolve(
            "INSERT INTO users (id, name) VALUES (1, 'Alice') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name",
            'INSERT',
            QueryKind::WRITE_SIMULATED
        );

        self::assertInstanceOf(UpsertMutation::class, $mutation);
        self::assertSame('users', $mutation->tableName());
    }

    public function testResolveInsertReturnsInsertMutationForValues(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $registry->register('users', new TableDefinition(
            ['id', 'name'],
            ['id' => 'INTEGER', 'name' => 'TEXT'],
            ['id'],
            [],
            []
        ));
        $mutation = $resolver->resolve(
            "INSERT INTO users (id, name) VALUES (1, 'Alice')",
            'INSERT',
            QueryKind::WRITE_SIMULATED
        );

        self::assertInstanceOf(InsertMutation::class, $mutation);
        self::assertSame('users', $mutation->tableName());
    }

    public function testResolveCreateTableLikeUnknownSourceThrows(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $this->expectException(\ZtdQuery\Exception\UnknownSchemaException::class);
        $resolver->resolve(
            'CREATE TABLE new_table (LIKE unknown_source)',
            'CREATE_TABLE',
            QueryKind::DDL_SIMULATED
        );
    }

    public function testResolveCreateTableAsSelectExtractsColumnNames(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $mutation = $resolver->resolve(
            'CREATE TABLE archive AS SELECT id, name FROM users',
            'CREATE_TABLE',
            QueryKind::DDL_SIMULATED
        );

        self::assertInstanceOf(CreateTableAsSelectMutation::class, $mutation);
        self::assertSame('archive', $mutation->tableName());
    }

    public function testResolveCreateTableAsSelectWithLowercaseSelectExtractsColumns(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $mutation = $resolver->resolve(
            'CREATE TABLE t AS select id, name from users',
            'CREATE_TABLE',
            QueryKind::DDL_SIMULATED
        );

        self::assertInstanceOf(CreateTableAsSelectMutation::class, $mutation);
    }

    public function testResolveCreateTableAsSelectWithNewlineBeforeFrom(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $mutation = $resolver->resolve(
            "CREATE TABLE t AS SELECT id,\nname\nFROM users",
            'CREATE_TABLE',
            QueryKind::DDL_SIMULATED
        );

        self::assertInstanceOf(CreateTableAsSelectMutation::class, $mutation);
    }

    public function testResolveCreateTableAsSelectStarReturnsEmptyColumns(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $mutation = $resolver->resolve(
            'CREATE TABLE t AS SELECT * FROM users',
            'CREATE_TABLE',
            QueryKind::DDL_SIMULATED
        );

        self::assertInstanceOf(CreateTableAsSelectMutation::class, $mutation);
    }

    public function testResolveCreateTableAsSelectWithAliasExtractsAliasName(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $mutation = $resolver->resolve(
            'CREATE TABLE t AS SELECT id, name as full_name FROM users',
            'CREATE_TABLE',
            QueryKind::DDL_SIMULATED
        );

        self::assertInstanceOf(CreateTableAsSelectMutation::class, $mutation);
    }

    public function testResolveCreateTableAsSelectExpressionColumn(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $mutation = $resolver->resolve(
            'CREATE TABLE t AS SELECT id + 1 FROM users',
            'CREATE_TABLE',
            QueryKind::DDL_SIMULATED
        );

        self::assertInstanceOf(CreateTableAsSelectMutation::class, $mutation);
    }

    public function testResolveCreateTableAsSelectWithQuotedStringContainingComma(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $mutation = $resolver->resolve(
            "CREATE TABLE t AS SELECT id, 'hello, world' as msg FROM users",
            'CREATE_TABLE',
            QueryKind::DDL_SIMULATED
        );

        self::assertInstanceOf(CreateTableAsSelectMutation::class, $mutation);
    }

    public function testResolveCreateTableAsSelectNoFromClause(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $mutation = $resolver->resolve(
            "CREATE TABLE t AS SELECT 1 as id, 'hello' as msg",
            'CREATE_TABLE',
            QueryKind::DDL_SIMULATED
        );

        self::assertInstanceOf(CreateTableAsSelectMutation::class, $mutation);
    }

    public function testResolveCreateTableAsSelectWithPaddedStar(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $mutation = $resolver->resolve(
            'CREATE TABLE t AS SELECT  *  FROM users',
            'CREATE_TABLE',
            QueryKind::DDL_SIMULATED
        );

        self::assertInstanceOf(CreateTableAsSelectMutation::class, $mutation);
    }

    public function testResolveCreateTableAsSelectWithDoubleQuotedAlias(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $mutation = $resolver->resolve(
            'CREATE TABLE t AS SELECT id, name AS "Full Name" FROM users',
            'CREATE_TABLE',
            QueryKind::DDL_SIMULATED
        );

        self::assertInstanceOf(CreateTableAsSelectMutation::class, $mutation);
    }

    public function testResolveCreateTableLikeWithKnownSource(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $registry->register('source_table', new TableDefinition(
            ['id', 'name'],
            ['id' => 'INTEGER', 'name' => 'TEXT'],
            ['id'],
            [],
            []
        ));
        $mutation = $resolver->resolve(
            'CREATE TABLE copy_table (LIKE source_table)',
            'CREATE_TABLE',
            QueryKind::DDL_SIMULATED
        );

        self::assertInstanceOf(CreateTableLikeMutation::class, $mutation);
        self::assertSame('copy_table', $mutation->tableName());
    }

    public function testResolveCreateTableAsSelectLowercaseSelectAndFrom(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $mutation = $resolver->resolve(
            'CREATE TABLE t AS select id, name from users',
            'CREATE_TABLE',
            QueryKind::DDL_SIMULATED
        );

        self::assertInstanceOf(CreateTableAsSelectMutation::class, $mutation);
    }

    public function testResolveCreateTableAsSelectWithLowercaseAliasAs(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $mutation = $resolver->resolve(
            'CREATE TABLE t AS SELECT id, name as full_name FROM users',
            'CREATE_TABLE',
            QueryKind::DDL_SIMULATED
        );

        self::assertInstanceOf(CreateTableAsSelectMutation::class, $mutation);
    }

    public function testResolveCreateTableAsSelectWithQuotedStringContainComma(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $mutation = $resolver->resolve(
            "CREATE TABLE t AS SELECT id, 'a,b' as val FROM users",
            'CREATE_TABLE',
            QueryKind::DDL_SIMULATED
        );

        self::assertInstanceOf(CreateTableAsSelectMutation::class, $mutation);
    }

    public function testResolveCreateTableAsSelectWithDoubleQuotedStringContainComma(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $mutation = $resolver->resolve(
            'CREATE TABLE t AS SELECT id, "col,name" FROM users',
            'CREATE_TABLE',
            QueryKind::DDL_SIMULATED
        );

        self::assertInstanceOf(CreateTableAsSelectMutation::class, $mutation);
    }

    public function testResolveCreateTableAlreadyExistsWithIfNotExists(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $registry->register('existing', new TableDefinition(
            ['id', 'name'],
            ['id' => 'INTEGER', 'name' => 'TEXT'],
            ['id'],
            [],
            []
        ));
        $mutation = $resolver->resolve(
            'CREATE TABLE IF NOT EXISTS existing (id INTEGER)',
            'CREATE_TABLE',
            QueryKind::DDL_SIMULATED
        );

        self::assertInstanceOf(CreateTableMutation::class, $mutation);
    }

    public function testResolveUpsertWithMultipleSetColumns(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $registry->register('users', new TableDefinition(
            ['id', 'name'],
            ['id' => 'INTEGER', 'name' => 'TEXT'],
            ['id'],
            [],
            []
        ));
        $mutation = $resolver->resolve(
            "INSERT INTO users (id, name) VALUES (1, 'Alice') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, id = EXCLUDED.id",
            'INSERT',
            QueryKind::WRITE_SIMULATED
        );

        self::assertInstanceOf(UpsertMutation::class, $mutation);
    }

    public function testResolveUpdateNoRegistryDoesNotCreateUnsafeMutation(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $this->expectException(UnknownSchemaException::class);
        $resolver->resolve(
            "UPDATE unknown_table SET name = 'Bob' WHERE id = 1",
            'UPDATE',
            QueryKind::WRITE_SIMULATED
        );
    }

    public function testResolveCreateTableAsSelectLowercaseSelectExtractsCorrectColumns(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $mutation = $resolver->resolve(
            'CREATE TABLE t AS select name from users',
            'CREATE_TABLE',
            QueryKind::DDL_SIMULATED
        );

        self::assertInstanceOf(CreateTableAsSelectMutation::class, $mutation);
        self::assertSame('t', $mutation->tableName());
    }

    public function testResolveCreateTableAsSelectNewlineBeforeFromExtractsColumns(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $mutation = $resolver->resolve(
            "CREATE TABLE t AS SELECT name\nFROM users",
            'CREATE_TABLE',
            QueryKind::DDL_SIMULATED
        );

        self::assertInstanceOf(CreateTableAsSelectMutation::class, $mutation);
        self::assertSame('t', $mutation->tableName());
    }

    public function testResolveCreateTableAsSelectWithLowercaseAliasAsV2(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $mutation = $resolver->resolve(
            'CREATE TABLE t AS SELECT id as user_id FROM users',
            'CREATE_TABLE',
            QueryKind::DDL_SIMULATED
        );

        self::assertInstanceOf(CreateTableAsSelectMutation::class, $mutation);
        self::assertSame('t', $mutation->tableName());
    }

    public function testResolveCreateTableAsSelectWithNoFromLowercaseSelect(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $mutation = $resolver->resolve(
            'CREATE TABLE t AS select 1 as val',
            'CREATE_TABLE',
            QueryKind::DDL_SIMULATED
        );

        self::assertInstanceOf(CreateTableAsSelectMutation::class, $mutation);
        self::assertSame('t', $mutation->tableName());
    }

    public function testResolveCreateTableAsSelectQuotedColumnAlias(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $mutation = $resolver->resolve(
            'CREATE TABLE t AS SELECT id AS "user_id" FROM users',
            'CREATE_TABLE',
            QueryKind::DDL_SIMULATED
        );

        self::assertInstanceOf(CreateTableAsSelectMutation::class, $mutation);
        self::assertSame('t', $mutation->tableName());
    }

    public function testResolveUpsertConflictValuesArePreserved(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $registry->register('users', new TableDefinition(
            ['id', 'name'],
            ['id' => 'INTEGER', 'name' => 'TEXT'],
            ['id'],
            [],
            []
        ));
        $mutation = $resolver->resolve(
            "INSERT INTO users (id, name) VALUES (1, 'Alice') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name",
            'INSERT',
            QueryKind::WRITE_SIMULATED
        );

        self::assertInstanceOf(UpsertMutation::class, $mutation);
    }

    public function testResolveUpsertTernaryRegisteredTableUsesDefinitionPrimaryKeys(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $registry->register('users', new TableDefinition(
            ['id', 'name'],
            ['id' => 'INTEGER', 'name' => 'TEXT'],
            ['id'],
            [],
            []
        ));
        $mutation = $resolver->resolve(
            "INSERT INTO users (id, name) VALUES (1, 'Alice') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name",
            'INSERT',
            QueryKind::WRITE_SIMULATED
        );

        self::assertInstanceOf(UpsertMutation::class, $mutation);
        self::assertSame('users', $mutation->tableName());
    }

    public function testResolveInsertFalseValueForHasInsertSelect(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $registry->register('users', new TableDefinition(
            ['id', 'name'],
            ['id' => 'INTEGER', 'name' => 'TEXT'],
            ['id'],
            [],
            []
        ));
        $mutation = $resolver->resolve(
            "INSERT INTO users (id, name) VALUES (1, 'Alice')",
            'INSERT',
            QueryKind::WRITE_SIMULATED
        );

        self::assertInstanceOf(InsertMutation::class, $mutation);
        self::assertSame('users', $mutation->tableName());
    }

    public function testResolveCreateTableAsSelectTableDotColumn(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $mutation = $resolver->resolve(
            'CREATE TABLE t AS SELECT users.name FROM users',
            'CREATE_TABLE',
            QueryKind::DDL_SIMULATED
        );

        self::assertInstanceOf(CreateTableAsSelectMutation::class, $mutation);
        self::assertSame('t', $mutation->tableName());
    }

    public function testResolveCtasApplyRegistersColumnsFromSelect(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $mutation = $resolver->resolve(
            'CREATE TABLE t AS SELECT id, name FROM users',
            'CREATE_TABLE',
            QueryKind::DDL_SIMULATED
        );
        self::assertNotNull($mutation);

        $mutation->apply($shadowStore, [['id' => 1, 'name' => 'Alice']]);
        $def = $registry->get('t');
        self::assertNotNull($def);
        self::assertSame(['id', 'name'], $def->columns);
    }

    public function testResolveCtasApplyLowercaseSelectRegistersColumns(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $mutation = $resolver->resolve(
            'CREATE TABLE t AS select id, name from users',
            'CREATE_TABLE',
            QueryKind::DDL_SIMULATED
        );
        self::assertNotNull($mutation);

        $mutation->apply($shadowStore, [['id' => 1, 'name' => 'Alice']]);
        $def = $registry->get('t');
        self::assertNotNull($def);
        self::assertSame(['id', 'name'], $def->columns);
    }

    public function testResolveCtasApplyWithAliasRegistersAliasName(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $mutation = $resolver->resolve(
            'CREATE TABLE t AS SELECT id AS user_id FROM users',
            'CREATE_TABLE',
            QueryKind::DDL_SIMULATED
        );
        self::assertNotNull($mutation);

        $mutation->apply($shadowStore, [['user_id' => 1]]);
        $def = $registry->get('t');
        self::assertNotNull($def);
        self::assertSame(['user_id'], $def->columns);
    }

    public function testResolveCtasApplyWithLowercaseAliasRegistersAliasName(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $mutation = $resolver->resolve(
            'CREATE TABLE t AS SELECT id as user_id FROM users',
            'CREATE_TABLE',
            QueryKind::DDL_SIMULATED
        );
        self::assertNotNull($mutation);

        $mutation->apply($shadowStore, [['user_id' => 1]]);
        $def = $registry->get('t');
        self::assertNotNull($def);
        self::assertSame(['user_id'], $def->columns);
    }

    public function testResolveCtasApplyWithQuotedAliasRegistersCorrectName(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $mutation = $resolver->resolve(
            'CREATE TABLE t AS SELECT id AS "user_id" FROM users',
            'CREATE_TABLE',
            QueryKind::DDL_SIMULATED
        );
        self::assertNotNull($mutation);

        $mutation->apply($shadowStore, [['user_id' => 1]]);
        $def = $registry->get('t');
        self::assertNotNull($def);
        self::assertSame(['user_id'], $def->columns);
    }

    public function testResolveCtasApplyNoFromLowercaseSelectRegistersColumns(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $mutation = $resolver->resolve(
            'CREATE TABLE t AS select 1 as val',
            'CREATE_TABLE',
            QueryKind::DDL_SIMULATED
        );
        self::assertNotNull($mutation);

        $mutation->apply($shadowStore, [['val' => 1]]);
        $def = $registry->get('t');
        self::assertNotNull($def);
        self::assertSame(['val'], $def->columns);
    }

    public function testResolveCtasApplySelectNewlineBeforeFromRegistersColumns(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $mutation = $resolver->resolve(
            "CREATE TABLE t AS SELECT id\nFROM users",
            'CREATE_TABLE',
            QueryKind::DDL_SIMULATED
        );
        self::assertNotNull($mutation);

        $mutation->apply($shadowStore, [['id' => 1]]);
        $def = $registry->get('t');
        self::assertNotNull($def);
        self::assertSame(['id'], $def->columns);
    }

    public function testResolveCtasApplyTableQualifiedColumnRegistersColumnName(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $mutation = $resolver->resolve(
            'CREATE TABLE t AS SELECT users.name FROM users',
            'CREATE_TABLE',
            QueryKind::DDL_SIMULATED
        );
        self::assertNotNull($mutation);

        $mutation->apply($shadowStore, [['name' => 'Alice']]);
        $def = $registry->get('t');
        self::assertNotNull($def);
        self::assertSame(['name'], $def->columns);
    }

    public function testResolveUpsertApplyUpdatesExistingRow(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );
        $registry->register('users', new TableDefinition(
            ['id', 'name'],
            ['id' => 'INTEGER', 'name' => 'TEXT'],
            ['id'],
            [],
            []
        ));
        $shadowStore->set('users', [['id' => 1, 'name' => 'Alice']]);

        $mutation = $resolver->resolve(
            "INSERT INTO users (id, name) VALUES (1, 'Bob') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name",
            'INSERT',
            QueryKind::WRITE_SIMULATED
        );
        self::assertNotNull($mutation);

        $mutation->apply($shadowStore, [['id' => 1, 'name' => 'Bob']]);
        $rows = $shadowStore->get('users');
        self::assertNotEmpty($rows);
        self::assertSame('Bob', $rows[0]['name']);
    }

    public function testResolveUpsertUsesPartialIndexPredicateForConflictDetection(): void
    {
        $shadowStore = new ShadowStore();
        $registry = new TableDefinitionRegistry();
        $definition = new TableDefinition(
            ['email', 'status', 'login_count'],
            ['email' => 'TEXT', 'status' => 'TEXT', 'login_count' => 'INTEGER'],
            [],
            [],
            [],
        );
        $registry->register('users', $definition->withPartialUniqueIndex(
            new PartialUniqueIndex('users_active_email', ['email'], "status = 'active'::text"),
        ));
        $shadowStore->set('users', [
            ['email' => 'alice@example.com', 'status' => 'inactive', 'login_count' => 2],
            ['email' => 'alice@example.com', 'status' => 'active', 'login_count' => 5],
        ]);
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser(),
        );
        $sql = "INSERT INTO users VALUES ('alice@example.com', 'active', 1) "
            . "ON CONFLICT (email) WHERE status = 'active' "
            . 'DO UPDATE SET login_count = users.login_count + EXCLUDED.login_count';

        $mutation = $resolver->resolve($sql, 'INSERT', QueryKind::WRITE_SIMULATED);
        self::assertInstanceOf(UpsertMutation::class, $mutation);
        $mutation->apply($shadowStore, [[
            'email' => 'alice@example.com',
            'status' => 'active',
            'login_count' => 1,
        ]]);

        self::assertSame([
            ['email' => 'alice@example.com', 'status' => 'inactive', 'login_count' => 2],
            ['email' => 'alice@example.com', 'status' => 'active', 'login_count' => 6],
        ], $shadowStore->get('users'));
    }

    public function testUpdateDoesNotTreatRowsFromUnknownInsertAsSchema(): void
    {
        $shadowStore = new ShadowStore();
        $shadowStore->insert('late_table', [['id' => 1, 'name' => 'Alice']]);
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            new TableDefinitionRegistry(),
            new PgSqlSchemaParser(),
            new PgSqlParser()
        );

        $this->expectException(UnknownSchemaException::class);
        $resolver->resolve(
            "UPDATE late_table SET name = 'Bob' WHERE id = 1",
            'UPDATE',
            QueryKind::WRITE_SIMULATED
        );
    }

    public function testResolveMergeReturnsAtomicTableSynchronization(): void
    {
        $shadowStore = new ShadowStore();
        $shadowStore->ensure('users');
        $registry = new TableDefinitionRegistry();
        $registry->register('users', new TableDefinition(
            ['id', 'name'],
            ['id' => 'INTEGER', 'name' => 'TEXT'],
            ['id'],
            ['id'],
            [],
        ));
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            $registry,
            new PgSqlSchemaParser(),
            new PgSqlParser(),
        );

        $mutation = $resolver->resolve(
            'MERGE INTO users u USING source s ON u.id = s.id WHEN MATCHED THEN DELETE',
            'MERGE',
            QueryKind::WRITE_SIMULATED,
        );

        self::assertInstanceOf(SynchronizeMutation::class, $mutation);
        self::assertSame('users', $mutation->tableName());
    }

    public function testResolveMergeAcceptsInitializedTableWithoutReflectedSchema(): void
    {
        $shadowStore = new ShadowStore();
        $shadowStore->ensure('users');
        $resolver = new PgSqlMutationResolver(
            $shadowStore,
            new TableDefinitionRegistry(),
            new PgSqlSchemaParser(),
            new PgSqlParser(),
        );

        $mutation = $resolver->resolve(
            'MERGE INTO users u USING source s ON u.id = s.id WHEN MATCHED THEN DELETE',
            'MERGE',
            QueryKind::WRITE_SIMULATED,
        );

        self::assertInstanceOf(SynchronizeMutation::class, $mutation);
        self::assertSame('users', $mutation->tableName());
    }

    public function testResolveMergeRejectsUnknownUninitializedTable(): void
    {
        $resolver = new PgSqlMutationResolver(
            new ShadowStore(),
            new TableDefinitionRegistry(),
            new PgSqlSchemaParser(),
            new PgSqlParser(),
        );

        $this->expectException(UnknownSchemaException::class);

        $resolver->resolve(
            'MERGE INTO users u USING source s ON u.id = s.id WHEN MATCHED THEN DELETE',
            'MERGE',
            QueryKind::WRITE_SIMULATED,
        );
    }
}
