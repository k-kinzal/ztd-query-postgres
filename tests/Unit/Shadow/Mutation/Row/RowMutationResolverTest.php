<?php

declare(strict_types=1);

namespace Tests\Unit\Shadow\Mutation\Row;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Shadow\Mutation\Row\RowMutationResolver::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Conflict\ColumnSet::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Cte\HeaderParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Cte\IdentifierReferences::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Cte\PrefixMerge::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Cte\ShadowDependencies::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\ComparisonOperator::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\ExpressionCursor::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\ExpressionLexeme::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\PrecedenceParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\PrimaryParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Merge\ActionClause::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Merge\BranchTokens::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Merge\RelationTarget::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Merge\StatementParts::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Relation\FromClause::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Relation\RelationReference::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Statement\Classification::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Statement\ConflictClause::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Statement\Identifiers::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Statement\InsertSource::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Statement\TableDefinitionClauses::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Schema\PgSqlColumnTypeMapper::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Conflict\PgSqlConflictTarget::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Cte\PgSqlCteShadowComposer::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Schema\Key\PgSqlForeignKeyDefinitionParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Merge\PgSqlMergeActionKind::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Merge\PgSqlMergeClause::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Merge\PgSqlMergeMatchKind::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Merge\PgSqlMergeParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Merge\PgSqlMergeStatement::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Partition\PgSqlPartitionParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Schema\PgSqlSchemaParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Relation\PgSqlSelectRelationParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\PgSqlUpsertExpressionParser::class)]
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
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Partition\StorageTable::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Lexing\CommentSpan::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Lexing\QuotedSpan::class)]
final class RowMutationResolverTest extends TestCase
{
    public function testResolveInsertAppliesOnlyReturnedRows(): void
    {
        $parser = new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser();
        $schemaParser = new \ZtdQuery\Platform\Postgres\Schema\PgSqlSchemaParser();
        $registry = new \ZtdQuery\Schema\TableDefinitionRegistry();
        $definition = $schemaParser->parse('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
        self::assertNotNull($definition);
        $registry->register('users', $definition);
        $store = new \ZtdQuery\Shadow\ShadowStore();
        $resolver = new \ZtdQuery\Platform\Postgres\Shadow\Mutation\Row\RowMutationResolver($parser, $registry, $store);
        $mutation = $resolver->resolveInsert("INSERT INTO users VALUES (1, 'Ada')");
        $mutation->apply($store, [['id' => 1, 'name' => 'Ada']]);
        self::assertSame('users', $mutation->tableName());
        self::assertSame([['id' => 1, 'name' => 'Ada']], $store->get('users'));
    }

    public function testResolveUpdatePreservesTheTargetIdentity(): void
    {
        $parser = new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser();
        $schemaParser = new \ZtdQuery\Platform\Postgres\Schema\PgSqlSchemaParser();
        $registry = new \ZtdQuery\Schema\TableDefinitionRegistry();
        $definition = $schemaParser->parse('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
        self::assertNotNull($definition);
        $registry->register('users', $definition);
        $store = new \ZtdQuery\Shadow\ShadowStore();
        $resolver = new \ZtdQuery\Platform\Postgres\Shadow\Mutation\Row\RowMutationResolver($parser, $registry, $store);
        $store->set('users', [['id' => 1, 'name' => 'before'], ['id' => 2, 'name' => 'untouched']]);
        $mutation = $resolver->resolveUpdate("UPDATE users SET name = 'after' WHERE id = 1");
        $mutation->apply($store, [['id' => 1, 'name' => 'after']]);
        self::assertSame([['id' => 1, 'name' => 'after'], ['id' => 2, 'name' => 'untouched']], $store->get('users'));
    }

    public function testResolveDeleteMatchesThePrimaryKey(): void
    {
        $parser = new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser();
        $schemaParser = new \ZtdQuery\Platform\Postgres\Schema\PgSqlSchemaParser();
        $registry = new \ZtdQuery\Schema\TableDefinitionRegistry();
        $definition = $schemaParser->parse('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
        self::assertNotNull($definition);
        $registry->register('users', $definition);
        $store = new \ZtdQuery\Shadow\ShadowStore();
        $resolver = new \ZtdQuery\Platform\Postgres\Shadow\Mutation\Row\RowMutationResolver($parser, $registry, $store);
        $store->set('users', [['id' => 1, 'name' => 'removed'], ['id' => 2, 'name' => 'kept']]);
        $mutation = $resolver->resolveDelete('DELETE FROM users WHERE id = 1');
        $mutation->apply($store, [['id' => 1, 'name' => 'removed']]);
        self::assertSame([['id' => 2, 'name' => 'kept']], $store->get('users'));
    }

    public function testResolveMergeSynchronizesTheProjectedTarget(): void
    {
        $parser = new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser();
        $schemaParser = new \ZtdQuery\Platform\Postgres\Schema\PgSqlSchemaParser();
        $registry = new \ZtdQuery\Schema\TableDefinitionRegistry();
        $definition = $schemaParser->parse('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
        self::assertNotNull($definition);
        $registry->register('users', $definition);
        $store = new \ZtdQuery\Shadow\ShadowStore();
        $resolver = new \ZtdQuery\Platform\Postgres\Shadow\Mutation\Row\RowMutationResolver($parser, $registry, $store);
        $mutation = $resolver->resolveMerge('MERGE INTO users u USING source s ON u.id = s.id WHEN MATCHED THEN DELETE');
        $mutation->apply($store, [['id' => 2, 'name' => 'kept']]);
        self::assertSame('users', $mutation->tableName());
        self::assertSame([['id' => 2, 'name' => 'kept']], $store->get('users'));
    }

    public function testResolveTruncateIncludesEveryExplicitTarget(): void
    {
        $parser = new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser();
        $schemaParser = new \ZtdQuery\Platform\Postgres\Schema\PgSqlSchemaParser();
        $registry = new \ZtdQuery\Schema\TableDefinitionRegistry();
        $definition = $schemaParser->parse('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
        self::assertNotNull($definition);
        $registry->register('users', $definition);
        $store = new \ZtdQuery\Shadow\ShadowStore();
        $resolver = new \ZtdQuery\Platform\Postgres\Shadow\Mutation\Row\RowMutationResolver($parser, $registry, $store);
        $store->set('users', [['id' => 1]]);
        $store->set('events', [['id' => 2]]);
        $mutation = $resolver->resolveTruncate('TRUNCATE users, events');
        $mutation->apply($store, []);
        self::assertSame([], $store->get('users'));
        self::assertSame([], $store->get('events'));
    }

    public function testResolveUpsertEvaluatesAssignmentsWithoutNativeCandidateKeys(): void
    {
        $parser = new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser();
        $schemaParser = new \ZtdQuery\Platform\Postgres\Schema\PgSqlSchemaParser();
        $registry = new \ZtdQuery\Schema\TableDefinitionRegistry();
        $definition = $schemaParser->parse('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
        self::assertNotNull($definition);
        $registry->register('users', $definition);
        $store = new \ZtdQuery\Shadow\ShadowStore();
        $resolver = new \ZtdQuery\Platform\Postgres\Shadow\Mutation\Row\RowMutationResolver($parser, $registry, $store);
        $store->set('users', [['id' => 1, 'name' => 'before']]);
        $mutation = $resolver->resolveUpsert("INSERT INTO users VALUES (1, 'incoming') ON CONFLICT (id) DO UPDATE SET name = 'after'", 'users', 'users', ['id'], ['columns' => ['name'], 'values' => ['name' => "'after'"]], null, null);
        $mutation->apply($store, [['id' => 1, 'name' => 'incoming']]);
        self::assertSame([['id' => 1, 'name' => 'after']], $store->get('users'));
    }
}
