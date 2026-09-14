<?php

declare(strict_types=1);

namespace Tests\Unit\Rewrite;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Rewrite\StatementRewriter::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Shadow\Mutation\Table\TableMutationResolver::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Shadow\Mutation\Row\RowMutationResolver::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Conflict\ColumnSet::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Cte\HeaderParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Cte\IdentifierReferences::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Cte\PrefixMerge::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Cte\ShadowDependencies::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Diagnostic\KeywordSearch::class)]
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
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Returning\ProjectionItem::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Sampling\SampleClause::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Sampling\SampleTokens::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Statement\Classification::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Statement\ConflictClause::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Statement\Identifiers::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Statement\InsertSource::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Statement\SelectColumns::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Statement\TableDefinitionClauses::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Value\PgSqlCastRenderer::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Schema\PgSqlColumnTypeMapper::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Conflict\PgSqlConflictTarget::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Cte\PgSqlCteShadowComposer::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Schema\Key\PgSqlForeignKeyDefinitionParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\GeneratedColumn\PgSqlGeneratedColumnProjector::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlIdentifierQuoter::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Merge\PgSqlMergeActionKind::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Merge\PgSqlMergeClause::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Merge\PgSqlMergeMatchKind::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Merge\PgSqlMergeParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Merge\PgSqlMergeStatement::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Shadow\PgSqlMutationResolver::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Upsert\PgSqlNativeUpsertProjector::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Partition\PgSqlPartitionParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Partition\PgSqlPartitionPredicateRenderer::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\PgSqlQueryGuard::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Diagnostic\PgSqlReadOnlyDiagnosticStatement::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Returning\PgSqlReturningProjectionParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Schema\PgSqlSchemaParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Relation\PgSqlSelectRelationParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Sampling\PgSqlTableSample::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Sampling\PgSqlTableSampleMethod::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Sampling\PgSqlTableSampleParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Sampling\PgSqlTableSampleRewriter::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Transformer\PgSqlTransformer::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\PgSqlUpsertExpressionParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Value\PgSqlValueRenderer::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\View\PgSqlViewShadowRenderer::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PostgreSqlLexicalMasker::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Sampling\SampleProjection::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Sampling\TableColumns::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Upsert\ConflictPredicate::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Upsert\ExpressionBinder::class)]
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
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Context\TableContext::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Value\CastTypeMapper::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Lexing\CommentSpan::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Lexing\QuotedSpan::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Value\NativeCastTarget::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Value\BinaryStream::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Value\LiteralText::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Transformer\Cte\RowSourceRenderer::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Transformer\DeleteTransformer::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Transformer\InsertRowRenderer::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Transformer\InsertSelectRenderer::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Transformer\InsertTransformer::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Transformer\Insert\ExpressionCast::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Transformer\Insert\OrderedExpressions::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Transformer\Insert\SelectProjection::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Transformer\Insert\UpsertProjection::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Transformer\Insert\ValueProjection::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Transformer\MergeTransformer::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Transformer\Merge\MatchConditions::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Transformer\Merge\RowActions::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Transformer\SelectTransformer::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Transformer\UpdateTransformer::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Transformer\Update\ColumnProjection::class)]
final class StatementRewriterTest extends TestCase
{
    public function testRewriteStatementShadowsRegisteredTables(): void
    {
        $parser = new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser();
        $schemaParser = new \ZtdQuery\Platform\Postgres\Schema\PgSqlSchemaParser();
        $registry = new \ZtdQuery\Schema\TableDefinitionRegistry();
        $definition = $schemaParser->parse('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
        self::assertNotNull($definition);
        $registry->register('users', $definition);
        $store = new \ZtdQuery\Shadow\ShadowStore();
        $views = new \ZtdQuery\Schema\ViewDefinitionSet();
        $guard = new \ZtdQuery\Platform\Postgres\Rewrite\PgSqlQueryGuard($parser);
        $select = new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\SelectTransformer();
        $transformer = new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\PgSqlTransformer($parser, $select, new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\InsertTransformer($parser, $select), new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\UpdateTransformer($parser, $select), new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\DeleteTransformer($parser, $select));
        $resolver = new \ZtdQuery\Platform\Postgres\Shadow\PgSqlMutationResolver($store, $registry, $schemaParser, $parser);
        $rewriter = new \ZtdQuery\Platform\Postgres\Rewrite\StatementRewriter(new \ZtdQuery\Platform\Postgres\Rewrite\Cte\PgSqlCteShadowComposer(), $guard, $resolver, $parser, new \ZtdQuery\Platform\Postgres\Rewrite\Partition\PgSqlPartitionPredicateRenderer(), $registry, new \ZtdQuery\Platform\Postgres\Rewrite\Returning\PgSqlReturningProjectionParser(), $store, $transformer, $views);
        $plan = $rewriter->rewriteStatement('SELECT * FROM users');
        self::assertSame(\ZtdQuery\Rewrite\QueryKind::READ, $plan->kind());
        self::assertStringContainsString('"users" AS MATERIALIZED', $plan->sql());
        self::assertNull($plan->mutation());
    }

    public function testValidateReadTablesRejectsUndeclaredRelations(): void
    {
        $parser = new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser();
        $schemaParser = new \ZtdQuery\Platform\Postgres\Schema\PgSqlSchemaParser();
        $registry = new \ZtdQuery\Schema\TableDefinitionRegistry();
        $definition = $schemaParser->parse('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
        self::assertNotNull($definition);
        $registry->register('users', $definition);
        $store = new \ZtdQuery\Shadow\ShadowStore();
        $views = new \ZtdQuery\Schema\ViewDefinitionSet();
        $guard = new \ZtdQuery\Platform\Postgres\Rewrite\PgSqlQueryGuard($parser);
        $select = new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\SelectTransformer();
        $transformer = new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\PgSqlTransformer($parser, $select, new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\InsertTransformer($parser, $select), new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\UpdateTransformer($parser, $select), new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\DeleteTransformer($parser, $select));
        $resolver = new \ZtdQuery\Platform\Postgres\Shadow\PgSqlMutationResolver($store, $registry, $schemaParser, $parser);
        $rewriter = new \ZtdQuery\Platform\Postgres\Rewrite\StatementRewriter(new \ZtdQuery\Platform\Postgres\Rewrite\Cte\PgSqlCteShadowComposer(), $guard, $resolver, $parser, new \ZtdQuery\Platform\Postgres\Rewrite\Partition\PgSqlPartitionPredicateRenderer(), $registry, new \ZtdQuery\Platform\Postgres\Rewrite\Returning\PgSqlReturningProjectionParser(), $store, $transformer, $views);
        $this->expectException(\ZtdQuery\Exception\UnknownSchemaException::class);
        $rewriter->validateReadTables('SELECT * FROM missing');
    }

    public function testRewriteMutationStagesDdlUntilItIsApplied(): void
    {
        $parser = new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser();
        $schemaParser = new \ZtdQuery\Platform\Postgres\Schema\PgSqlSchemaParser();
        $registry = new \ZtdQuery\Schema\TableDefinitionRegistry();
        $definition = $schemaParser->parse('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
        self::assertNotNull($definition);
        $registry->register('users', $definition);
        $store = new \ZtdQuery\Shadow\ShadowStore();
        $views = new \ZtdQuery\Schema\ViewDefinitionSet();
        $guard = new \ZtdQuery\Platform\Postgres\Rewrite\PgSqlQueryGuard($parser);
        $select = new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\SelectTransformer();
        $transformer = new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\PgSqlTransformer($parser, $select, new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\InsertTransformer($parser, $select), new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\UpdateTransformer($parser, $select), new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\DeleteTransformer($parser, $select));
        $resolver = new \ZtdQuery\Platform\Postgres\Shadow\PgSqlMutationResolver($store, $registry, $schemaParser, $parser);
        $rewriter = new \ZtdQuery\Platform\Postgres\Rewrite\StatementRewriter(new \ZtdQuery\Platform\Postgres\Rewrite\Cte\PgSqlCteShadowComposer(), $guard, $resolver, $parser, new \ZtdQuery\Platform\Postgres\Rewrite\Partition\PgSqlPartitionPredicateRenderer(), $registry, new \ZtdQuery\Platform\Postgres\Rewrite\Returning\PgSqlReturningProjectionParser(), $store, $transformer, $views);
        $plan = $rewriter->rewriteMutation('CREATE TABLE events (id INT)', \ZtdQuery\Rewrite\QueryKind::DDL_SIMULATED, 'CREATE_TABLE', []);
        self::assertSame('SELECT 1 WHERE FALSE', $plan->sql());
        self::assertFalse($registry->has('events'));
        $mutation = $plan->mutation();
        self::assertNotNull($mutation);
        $mutation->apply($store, []);
        self::assertTrue($registry->has('events'));
    }
}
