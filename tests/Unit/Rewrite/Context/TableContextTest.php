<?php

declare(strict_types=1);

namespace Tests\Unit\Rewrite\Context;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Rewrite\Context\TableContext::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Conflict\ColumnSet::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Relation\FromClause::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Relation\RelationReference::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Statement\Classification::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Statement\ConflictClause::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Statement\Identifiers::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Statement\InsertSource::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Statement\TableDefinitionClauses::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Schema\PgSqlColumnTypeMapper::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Conflict\PgSqlConflictTarget::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Schema\Key\PgSqlForeignKeyDefinitionParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlIdentifierQuoter::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Partition\PgSqlPartitionParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Partition\PgSqlPartitionPredicateRenderer::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Schema\PgSqlSchemaParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Relation\PgSqlSelectRelationParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\View\PgSqlViewShadowRenderer::class)]
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
final class TableContextTest extends TestCase
{
    public function testBuildTableContextIncludesShadowTablesAndViews(): void
    {
        $parser = new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser();
        $schemaParser = new \ZtdQuery\Platform\Postgres\Schema\PgSqlSchemaParser();
        $registry = new \ZtdQuery\Schema\TableDefinitionRegistry();
        $definition = $schemaParser->parse('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
        self::assertNotNull($definition);
        $registry->register('users', $definition);
        $store = new \ZtdQuery\Shadow\ShadowStore();
        $views = new \ZtdQuery\Schema\ViewDefinitionSet();
        $context = new \ZtdQuery\Platform\Postgres\Rewrite\Context\TableContext(new \ZtdQuery\Platform\Postgres\Rewrite\Partition\PgSqlPartitionPredicateRenderer(), $registry, $store, $views);
        $store->set('users', [['id' => 1, 'name' => 'Ada']]);
        $views->register('named_users', new \ZtdQuery\Schema\ViewDefinition('SELECT name FROM users', ['users']));
        $tables = $context->buildTableContext($store->getAll());
        self::assertArrayHasKey('rows', $tables['users']);
        self::assertSame([['id' => 1, 'name' => 'Ada']], $tables['users']['rows'] ?? null);
        self::assertSame(['id', 'name'], $tables['users']['columns']);
        self::assertArrayHasKey('viewSql', $tables['named_users']);
    }

    public function testTableExistsIncludesRegisteredMaterializedAndViewNames(): void
    {
        $parser = new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser();
        $schemaParser = new \ZtdQuery\Platform\Postgres\Schema\PgSqlSchemaParser();
        $registry = new \ZtdQuery\Schema\TableDefinitionRegistry();
        $definition = $schemaParser->parse('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
        self::assertNotNull($definition);
        $registry->register('users', $definition);
        $store = new \ZtdQuery\Shadow\ShadowStore();
        $views = new \ZtdQuery\Schema\ViewDefinitionSet();
        $context = new \ZtdQuery\Platform\Postgres\Rewrite\Context\TableContext(new \ZtdQuery\Platform\Postgres\Rewrite\Partition\PgSqlPartitionPredicateRenderer(), $registry, $store, $views);
        $store->set('scratch', [['value' => 1]]);
        $views->register('named_users', new \ZtdQuery\Schema\ViewDefinition('SELECT name FROM users', ['users']));
        self::assertTrue($context->tableExists('users'));
        self::assertTrue($context->tableExists('scratch'));
        self::assertTrue($context->tableExists('named_users'));
        self::assertFalse($context->tableExists('missing'));
    }

    public function testHasSchemaContextTracksMutableRegistryState(): void
    {
        $parser = new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser();
        $schemaParser = new \ZtdQuery\Platform\Postgres\Schema\PgSqlSchemaParser();
        $registry = new \ZtdQuery\Schema\TableDefinitionRegistry();
        $definition = $schemaParser->parse('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
        self::assertNotNull($definition);
        $registry->register('users', $definition);
        $store = new \ZtdQuery\Shadow\ShadowStore();
        $views = new \ZtdQuery\Schema\ViewDefinitionSet();
        $context = new \ZtdQuery\Platform\Postgres\Rewrite\Context\TableContext(new \ZtdQuery\Platform\Postgres\Rewrite\Partition\PgSqlPartitionPredicateRenderer(), $registry, $store, $views);
        self::assertTrue($context->hasSchemaContext());
        $registry->clear();
        self::assertFalse($context->hasSchemaContext());
        $store->set('scratch', []);
        self::assertTrue($context->hasSchemaContext());
    }

    public function testDataContextInfersTheUnionOfUnknownRowColumns(): void
    {
        $parser = new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser();
        $schemaParser = new \ZtdQuery\Platform\Postgres\Schema\PgSqlSchemaParser();
        $registry = new \ZtdQuery\Schema\TableDefinitionRegistry();
        $definition = $schemaParser->parse('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
        self::assertNotNull($definition);
        $registry->register('users', $definition);
        $store = new \ZtdQuery\Shadow\ShadowStore();
        $views = new \ZtdQuery\Schema\ViewDefinitionSet();
        $context = new \ZtdQuery\Platform\Postgres\Rewrite\Context\TableContext(new \ZtdQuery\Platform\Postgres\Rewrite\Partition\PgSqlPartitionPredicateRenderer(), $registry, $store, $views);
        $tables = $context->dataContext(['scratch' => [['first' => 1], ['second' => 'two']]]);
        self::assertSame(['first', 'second'], $tables['scratch']['columns']);
        self::assertSame([['first' => 1], ['second' => 'two']], $tables['scratch']['rows']);
    }

    public function testFillDefinitionsRetainsExistingRowsAndAddsUnmaterializedTables(): void
    {
        $parser = new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser();
        $schemaParser = new \ZtdQuery\Platform\Postgres\Schema\PgSqlSchemaParser();
        $registry = new \ZtdQuery\Schema\TableDefinitionRegistry();
        $definition = $schemaParser->parse('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
        self::assertNotNull($definition);
        $registry->register('users', $definition);
        $store = new \ZtdQuery\Shadow\ShadowStore();
        $views = new \ZtdQuery\Schema\ViewDefinitionSet();
        $context = new \ZtdQuery\Platform\Postgres\Rewrite\Context\TableContext(new \ZtdQuery\Platform\Postgres\Rewrite\Partition\PgSqlPartitionPredicateRenderer(), $registry, $store, $views);
        $data = $context->dataContext(['scratch' => [['value' => 1]]]);
        $tables = $context->fillDefinitions($data);
        self::assertSame([['value' => 1]], $tables['scratch']['rows']);
        self::assertSame([], $tables['users']['rows'] ?? null);
        self::assertSame(['id', 'name'], $tables['users']['columns']);
    }

    public function testApplyPartitionsUsesSharedParentRows(): void
    {
        $parser = new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser();
        $schemaParser = new \ZtdQuery\Platform\Postgres\Schema\PgSqlSchemaParser();
        $registry = new \ZtdQuery\Schema\TableDefinitionRegistry();
        $definition = $schemaParser->parse('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
        self::assertNotNull($definition);
        $registry->register('users', $definition);
        $store = new \ZtdQuery\Shadow\ShadowStore();
        $views = new \ZtdQuery\Schema\ViewDefinitionSet();
        $context = new \ZtdQuery\Platform\Postgres\Rewrite\Context\TableContext(new \ZtdQuery\Platform\Postgres\Rewrite\Partition\PgSqlPartitionPredicateRenderer(), $registry, $store, $views);
        $registry->register('users_low', $definition->withPartitionRelation(new \ZtdQuery\Schema\Partition\TablePartitionRelation('users', 'id < 10')));
        $data = ['users' => [['id' => 1, 'name' => 'Ada']]];
        $tables = $context->applyPartitions($context->fillDefinitions($context->dataContext($data)), $data);
        self::assertSame($data['users'], $tables['users_low']['rows']);
        self::assertArrayHasKey('storageTable', $tables['users_low']);
        self::assertArrayHasKey('sourceSql', $tables['users_low']);
        self::assertSame('users', $tables['users_low']['storageTable'] ?? null);
        self::assertSame('SELECT * FROM "users" WHERE id < 10', $tables['users_low']['sourceSql'] ?? null);
    }
}
