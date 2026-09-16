<?php

declare(strict_types=1);

namespace Tests\Unit\Shadow\Mutation\Table;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Shadow\Mutation\Table\TableMutationResolver::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Conflict\ColumnSet::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Relation\FromClause::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Relation\RelationReference::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Statement\StatementClassifier::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Statement\ConflictClause::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Lexing\IdentifierDecoder::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Statement\TargetTableParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Dml\Update\UpdateClauseParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Dml\Delete\DeleteClauseParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Dml\Insert\InsertClauseParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Dml\Insert\InsertClauseParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Statement\SelectColumns::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Statement\TableDefinitionClauses::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Schema\PgSqlColumnTypeMapper::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Conflict\PgSqlConflictTarget::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Schema\Key\PgSqlForeignKeyDefinitionParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Partition\PgSqlPartitionParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Schema\PgSqlSchemaParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Relation\PgSqlSelectRelationParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PostgreSqlLexicalMasker::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Schema\Definition\ColumnDefinition::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Schema\Definition\ColumnTypeDeclaration::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Schema\Definition\TableBody::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Schema\Definition\TableConstraint::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Schema\Definition\TableFields::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Schema\Key\DefinitionEntry::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Schema\Key\DefinitionTokens::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Partition\BoundPredicate::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Partition\ClauseTokens::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Lexing\CommentSpan::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Lexing\QuotedSpan::class)]
final class TableMutationResolverTest extends TestCase
{
    public function testResolveCreateTableRegistersItsSchemaOnlyWhenApplied(): void
    {
        $parser = new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser();
        $schemaParser = new \ZtdQuery\Platform\Postgres\Schema\PgSqlSchemaParser();
        $registry = new \ZtdQuery\Schema\TableDefinitionRegistry();
        $definition = $schemaParser->parse('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
        self::assertNotNull($definition);
        $registry->register('users', $definition);
        $store = new \ZtdQuery\Shadow\ShadowStore();
        $resolver = new \ZtdQuery\Platform\Postgres\Shadow\Mutation\Table\TableMutationResolver($parser, new \ZtdQuery\Platform\Postgres\Sql\Partition\PgSqlPartitionParser(), $registry, $schemaParser);
        $mutation = $resolver->resolveCreateTable('CREATE TABLE events (id INTEGER PRIMARY KEY)');
        self::assertFalse($registry->has('events'));
        $mutation->apply($store, []);
        self::assertSame('events', $mutation->tableName());
        self::assertTrue($registry->has('events'));
        self::assertSame([], $store->get('events'));
    }

    public function testResolveDropTableRemovesOnlyTheTarget(): void
    {
        $parser = new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser();
        $schemaParser = new \ZtdQuery\Platform\Postgres\Schema\PgSqlSchemaParser();
        $registry = new \ZtdQuery\Schema\TableDefinitionRegistry();
        $definition = $schemaParser->parse('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
        self::assertNotNull($definition);
        $registry->register('users', $definition);
        $store = new \ZtdQuery\Shadow\ShadowStore();
        $resolver = new \ZtdQuery\Platform\Postgres\Shadow\Mutation\Table\TableMutationResolver($parser, new \ZtdQuery\Platform\Postgres\Sql\Partition\PgSqlPartitionParser(), $registry, $schemaParser);
        $store->set('users', [['id' => 1, 'name' => 'Ada']]);
        $mutation = $resolver->resolveDropTable('DROP TABLE users');
        self::assertTrue($registry->has('users'));
        $mutation->apply($store, []);
        self::assertFalse($registry->has('users'));
        self::assertFalse($store->has('users'));
    }

    public function testResolveAlterTableRejectsUnsupportedSchemaChanges(): void
    {
        $parser = new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser();
        $schemaParser = new \ZtdQuery\Platform\Postgres\Schema\PgSqlSchemaParser();
        $registry = new \ZtdQuery\Schema\TableDefinitionRegistry();
        $definition = $schemaParser->parse('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
        self::assertNotNull($definition);
        $registry->register('users', $definition);
        $store = new \ZtdQuery\Shadow\ShadowStore();
        $resolver = new \ZtdQuery\Platform\Postgres\Shadow\Mutation\Table\TableMutationResolver($parser, new \ZtdQuery\Platform\Postgres\Sql\Partition\PgSqlPartitionParser(), $registry, $schemaParser);
        $this->expectException(\ZtdQuery\Exception\UnsupportedSqlException::class);
        $resolver->resolveAlterTable('ALTER TABLE users ADD COLUMN age INTEGER');
    }

    public function testResolvePartitionInheritsItsParentDefinition(): void
    {
        $parser = new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser();
        $schemaParser = new \ZtdQuery\Platform\Postgres\Schema\PgSqlSchemaParser();
        $registry = new \ZtdQuery\Schema\TableDefinitionRegistry();
        $definition = $schemaParser->parse('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
        self::assertNotNull($definition);
        $registry->register('users', $definition);
        $store = new \ZtdQuery\Shadow\ShadowStore();
        $resolver = new \ZtdQuery\Platform\Postgres\Shadow\Mutation\Table\TableMutationResolver($parser, new \ZtdQuery\Platform\Postgres\Sql\Partition\PgSqlPartitionParser(), $registry, $schemaParser);
        $registry->register('users', $definition->withPartitionKey(new \ZtdQuery\Schema\Partition\TablePartitionKey(\ZtdQuery\Schema\Partition\TablePartitionStrategy::Range, ['id'])));
        $mutation = $resolver->resolvePartition('CREATE TABLE users_low PARTITION OF users FOR VALUES FROM (0) TO (10)', 'users_low', 'users', false);
        $mutation->apply($store, []);
        $child = $registry->get('users_low');
        self::assertNotNull($child);
        self::assertSame(['id', 'name'], $child->columns);
        self::assertSame('users', $child->partitionRelation?->parentTable);
    }
}
