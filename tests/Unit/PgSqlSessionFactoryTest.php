<?php

declare(strict_types=1);

namespace Tests\Unit;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Config\ZtdConfig;
use ZtdQuery\Connection\ConnectionInterface;
use ZtdQuery\Connection\StatementInterface;
use ZtdQuery\Platform\Postgres\Connection\Copy\PgSqlCopySupport;
use ZtdQuery\Platform\Postgres\Connection\Parameter\PgSqlPdoParameterBindingCompiler;
use ZtdQuery\Platform\Postgres\Connection\Parameter\PgSqlPdoPlaceholderEscaper;
use ZtdQuery\Platform\Postgres\Connection\Result\PgSqlPdoResultColumnTypeResolver;
use ZtdQuery\Platform\Postgres\PgSqlSessionFactory;
use ZtdQuery\Platform\Postgres\Rewrite\PgSqlQueryGuard;
use ZtdQuery\Platform\Postgres\Rewrite\PgSqlRewriter;
use ZtdQuery\Platform\Postgres\Rewrite\Transformer\DeleteTransformer;
use ZtdQuery\Platform\Postgres\Rewrite\Transformer\InsertTransformer;
use ZtdQuery\Platform\Postgres\Rewrite\Transformer\PgSqlTransformer;
use ZtdQuery\Platform\Postgres\Rewrite\Transformer\SelectTransformer;
use ZtdQuery\Platform\Postgres\Rewrite\Transformer\UpdateTransformer;
use ZtdQuery\Platform\Postgres\Schema\Partition\PgSqlPartitionReflector;
use ZtdQuery\Platform\Postgres\Schema\PgSqlSchemaParser;
use ZtdQuery\Platform\Postgres\Schema\PgSqlSchemaReflector;
use ZtdQuery\Platform\Postgres\Shadow\PgSqlMutationResolver;
use ZtdQuery\Platform\Postgres\Sql\Partition\PgSqlPartitionParser;
use ZtdQuery\Platform\Postgres\Sql\PgSqlIdentifierQuoter;
use ZtdQuery\Platform\Postgres\Sql\PgSqlParser;
use ZtdQuery\Platform\Postgres\Sql\Value\PgSqlCastRenderer;

#[CoversClass(PgSqlSessionFactory::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Schema\PgSqlColumnTypeMapper::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Schema\Key\PgSqlForeignKeyDefinitionParser::class)]
#[UsesClass(PgSqlParser::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Conflict\PgSqlConflictTarget::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\PostgreSqlLexicalMasker::class)]
#[UsesClass(PgSqlSchemaParser::class)]
#[UsesClass(PgSqlPartitionParser::class)]
#[UsesClass(PgSqlPartitionReflector::class)]
#[UsesClass(PgSqlQueryGuard::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Diagnostic\PgSqlReadOnlyDiagnosticStatement::class)]
#[UsesClass(PgSqlRewriter::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Relation\PgSqlSelectRelationParser::class)]
#[UsesClass(PgSqlMutationResolver::class)]
#[UsesClass(PgSqlTransformer::class)]
#[UsesClass(PgSqlSchemaReflector::class)]
#[UsesClass(PgSqlCastRenderer::class)]
#[UsesClass(PgSqlCopySupport::class)]
#[UsesClass(PgSqlPdoParameterBindingCompiler::class)]
#[UsesClass(PgSqlPdoPlaceholderEscaper::class)]
#[UsesClass(PgSqlPdoResultColumnTypeResolver::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Value\PgSqlValueRenderer::class)]
#[UsesClass(PgSqlIdentifierQuoter::class)]
#[UsesClass(SelectTransformer::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Sampling\PgSqlTableSampleParser::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Sampling\PgSqlTableSampleRewriter::class)]
#[UsesClass(InsertTransformer::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Transformer\InsertRowRenderer::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Transformer\InsertSelectRenderer::class)]
#[UsesClass(UpdateTransformer::class)]
#[UsesClass(DeleteTransformer::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Cte\PgSqlCteShadowComposer::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Upsert\PgSqlNativeUpsertProjector::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Schema\View\PgSqlViewDefinitionParser::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\View\PgSqlViewShadowRenderer::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\GeneratedColumn\PgSqlGeneratedColumnProjector::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Partition\PgSqlPartitionPredicateRenderer::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Returning\PgSqlReturningProjectionParser::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\PgSqlUpsertExpressionParser::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::class)]
#[CoversClass(\ZtdQuery\Platform\Postgres\Session\SchemaInitializer::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Connection\Copy\TargetColumns::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Connection\Copy\TargetSql::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Connection\Copy\TextFields::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Connection\Parameter\EscapeCursor::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Connection\Parameter\OperandInput::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Connection\Parameter\QuotedInput::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Connection\Parameter\TokenBoundary::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Shadow\Mutation\Table\TableMutationResolver::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Shadow\Mutation\Row\RowMutationResolver::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Conflict\ColumnSet::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Cte\HeaderParser::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Cte\IdentifierReferences::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Cte\PrefixMerge::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Cte\ShadowDependencies::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Diagnostic\KeywordSearch::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\ComparisonOperator::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\ExpressionCursor::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\ExpressionLexeme::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\PrecedenceParser::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\PrimaryParser::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Merge\ActionClause::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Merge\BranchTokens::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Merge\RelationTarget::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Merge\StatementParts::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Relation\FromClause::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Relation\RelationReference::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Returning\ProjectionItem::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Sampling\SampleClause::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Sampling\SampleTokens::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Statement\StatementClassifier::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Statement\ConflictClause::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Lexing\IdentifierDecoder::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Statement\TargetTableParser::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Dml\Update\UpdateClauseParser::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Dml\Delete\DeleteClauseParser::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Dml\Insert\InsertClauseParser::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Dml\Insert\InsertClauseParser::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Statement\SelectColumns::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Statement\TableDefinitionClauses::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Transaction\KeywordForm::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Merge\PgSqlMergeActionKind::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Merge\PgSqlMergeClause::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Merge\PgSqlMergeMatchKind::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Merge\PgSqlMergeParser::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Merge\PgSqlMergeStatement::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Sampling\PgSqlTableSample::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Sampling\PgSqlTableSampleMethod::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Transaction\PgSqlTransactionStatementParser::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Schema\Reflection\Catalog\PartitionKeys::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Schema\Reflection\Catalog\TableQueries::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Schema\Reflection\Column\ColumnDefinitionSql::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Schema\Reflection\Column\NativeTypeSql::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Schema\Reflection\Key\ForeignKeyRow::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Schema\Reflection\Key\ForeignKeys::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Schema\Reflection\Key\IndexDefinitions::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Schema\Reflection\Key\IndexRows::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Schema\Reflection\Key\PrimaryColumns::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Schema\Reflection\Key\UniqueIndexes::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Sampling\SampleProjection::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Sampling\TableColumns::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Upsert\ConflictPredicate::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Upsert\ExpressionBinder::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Schema\Definition\ColumnDefinition::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Schema\Definition\ColumnTypeDeclaration::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Schema\Definition\TableBody::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Schema\Definition\TableConstraint::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Schema\Definition\TableFields::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Schema\Key\DefinitionEntry::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Schema\Key\DefinitionTokens::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Partition\BoundPredicate::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Partition\ClauseTokens::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Partition\StorageTable::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Context\TableContext::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\StatementRewriter::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Value\CastTypeMapper::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Lexing\CommentSpan::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Lexing\QuotedSpan::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Value\NativeCastTarget::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Value\BinaryStream::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Value\LiteralText::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Transformer\Cte\RowSourceRenderer::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Transformer\Insert\ExpressionCast::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Transformer\Insert\OrderedExpressions::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Transformer\Insert\SelectProjection::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Transformer\Insert\UpsertProjection::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Transformer\Insert\ValueProjection::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Transformer\MergeTransformer::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Transformer\Merge\MatchConditions::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Transformer\Merge\RowActions::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Transformer\Update\ColumnProjection::class)]
final class PgSqlSessionFactoryTest extends TestCase
{
    public function testCreateRegistersReflectedPartitionMetadata(): void
    {
        $tables = self::createStub(StatementInterface::class);
        $tables->method('fetchAll')->willReturn([['table_name' => 'logs'], ['table_name' => 'logs_2024']]);
        $columns = self::createStub(StatementInterface::class);
        $columns->method('fetchAll')->willReturn([['column_name' => 'id', 'data_type' => 'integer', 'character_maximum_length' => null, 'numeric_precision' => 32, 'numeric_scale' => 0, 'is_nullable' => 'NO', 'column_default' => null, 'udt_name' => 'int4'], ['column_name' => 'log_date', 'data_type' => 'date', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'NO', 'column_default' => null, 'udt_name' => 'date']]);
        $primaryKey = self::createStub(StatementInterface::class);
        $primaryKey->method('fetchAll')->willReturn([['column_name' => 'id'], ['column_name' => 'log_date']]);
        $empty = self::createStub(StatementInterface::class);
        $empty->method('fetchAll')->willReturn([]);
        $partitionKeys = self::createStub(StatementInterface::class);
        $partitionKeys->method('fetchAll')->willReturn([['table_name' => 'logs', 'partition_key' => 'RANGE (log_date)']]);
        $relations = self::createStub(StatementInterface::class);
        $relations->method('fetchAll')->willReturn([['child_table' => 'logs_2024', 'parent_table' => 'logs', 'predicate' => "log_date >= '2024-01-01'::date AND log_date < '2025-01-01'::date"]]);
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function (string $sql) use ($tables, $columns, $primaryKey, $empty, $partitionKeys, $relations) {
            return match (true) {
                str_contains($sql, 'information_schema.tables') => $tables,
                str_contains($sql, 'information_schema.columns') => $columns,
                str_contains($sql, "constraint_type = 'PRIMARY KEY'") => $primaryKey,
                str_contains($sql, 'SELECT c.relname AS table_name') => $partitionKeys,
                str_contains($sql, 'SELECT child.relname AS child_table') => $relations,
                default => $empty,
            };
        });
        $session = (new PgSqlSessionFactory())->create($connection, ZtdConfig::default());
        $sql = $session->rewrite('SELECT * FROM logs_2024')->sql();
        self::assertStringContainsString('"logs" AS MATERIALIZED', $sql);
        self::assertStringContainsString('"logs_2024" AS MATERIALIZED (SELECT * FROM "logs" WHERE log_date >=', $sql);
        $create = $session->rewrite("CREATE TABLE logs_2025 PARTITION OF logs FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')");
        self::assertNotNull($create->mutation());
    }
    public function testCreateRegistersReflectedViews(): void
    {
        $empty = self::createStub(StatementInterface::class);
        $empty->method('fetchAll')->willReturn([]);
        $views = self::createStub(StatementInterface::class);
        $views->method('fetchAll')->willReturn([['viewname' => 'active_users', 'definition' => 'SELECT 1 AS id']]);
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static fn (string $sql): StatementInterface => str_contains($sql, 'pg_views') ? $views : $empty);
        $session = (new PgSqlSessionFactory())->create($connection, ZtdConfig::default());
        self::assertSame("WITH \"active_users\" AS MATERIALIZED (SELECT 1 AS id)\nSELECT * FROM active_users", $session->rewrite('SELECT * FROM active_users')->sql());
    }
    public function testCreateReturnsSession(): void
    {
        $connection = static::createStub(ConnectionInterface::class);
        $tablesStmt = static::createStub(StatementInterface::class);
        $tablesStmt->method('fetchAll')->willReturn([]);
        $connection->method('query')->willReturn($tablesStmt);
        $factory = new PgSqlSessionFactory();
        $session = $factory->create($connection, ZtdConfig::default());
        self::assertTrue($session->isEnabled());
        self::assertInstanceOf(PgSqlCopySupport::class, $session->copySupport());
        self::assertInstanceOf(PgSqlPdoParameterBindingCompiler::class, $session->parameterBindingCompiler());
        self::assertInstanceOf(PgSqlPdoResultColumnTypeResolver::class, $session->resultColumnTypeResolver());
    }
    public function testCreatedSessionIsEnabledByDefault(): void
    {
        $connection = static::createStub(ConnectionInterface::class);
        $tablesStmt = static::createStub(StatementInterface::class);
        $tablesStmt->method('fetchAll')->willReturn([]);
        $connection->method('query')->willReturn($tablesStmt);
        $factory = new PgSqlSessionFactory();
        $session = $factory->create($connection, ZtdConfig::default());
        self::assertTrue($session->isEnabled());
    }
    public function testCreateWithTablesReflectsSchema(): void
    {
        $connection = static::createStub(ConnectionInterface::class);
        $tablesStmt = static::createStub(StatementInterface::class);
        $tablesStmt->method('fetchAll')->willReturn([['table_name' => 'users']]);
        $columnsStmt = static::createStub(StatementInterface::class);
        $columnsStmt->method('fetchAll')->willReturn([['column_name' => 'id', 'data_type' => 'INTEGER', 'character_maximum_length' => null, 'numeric_precision' => 32, 'numeric_scale' => 0, 'is_nullable' => 'NO', 'column_default' => null, 'udt_name' => 'int4'], ['column_name' => 'name', 'data_type' => 'TEXT', 'character_maximum_length' => null, 'numeric_precision' => null, 'numeric_scale' => null, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'text']]);
        $pkStmt = static::createStub(StatementInterface::class);
        $pkStmt->method('fetchAll')->willReturn([['column_name' => 'id']]);
        $uniqueStmt = static::createStub(StatementInterface::class);
        $uniqueStmt->method('fetchAll')->willReturn([]);
        $connection->method('query')->willReturnCallback(function (string $sql) use ($tablesStmt, $columnsStmt, $pkStmt, $uniqueStmt) {
            if (str_contains($sql, 'information_schema.tables')) {
                return $tablesStmt;
            }
            if (str_contains($sql, 'information_schema.columns')) {
                return $columnsStmt;
            }
            if (str_contains($sql, "constraint_type = 'PRIMARY KEY'")) {
                return $pkStmt;
            }
            if (str_contains($sql, "constraint_type = 'UNIQUE'")) {
                return $uniqueStmt;
            }
            return false;
        });
        $factory = new PgSqlSessionFactory();
        $session = $factory->create($connection, ZtdConfig::default());
        self::assertTrue($session->isEnabled());
    }
    public function testCreateWithEmptyDatabaseReturnsSession(): void
    {
        $connection = static::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturn(false);
        $factory = new PgSqlSessionFactory();
        $session = $factory->create($connection, ZtdConfig::default());
        self::assertTrue($session->isEnabled());
    }
    public function testSessionCanBeEnabledAfterCreation(): void
    {
        $connection = static::createStub(ConnectionInterface::class);
        $tablesStmt = static::createStub(StatementInterface::class);
        $tablesStmt->method('fetchAll')->willReturn([]);
        $connection->method('query')->willReturn($tablesStmt);
        $factory = new PgSqlSessionFactory();
        $session = $factory->create($connection, ZtdConfig::default());
        $session->enable();
        self::assertTrue($session->isEnabled());
    }
    public function testCreateWithTableRegistersSchemaInSession(): void
    {
        $connection = static::createStub(ConnectionInterface::class);
        $tablesStmt = static::createStub(StatementInterface::class);
        $tablesStmt->method('fetchAll')->willReturn([['table_name' => 'products']]);
        $columnsStmt = static::createStub(StatementInterface::class);
        $columnsStmt->method('fetchAll')->willReturn([['column_name' => 'id', 'data_type' => 'INTEGER', 'character_maximum_length' => null, 'numeric_precision' => 32, 'numeric_scale' => 0, 'is_nullable' => 'NO', 'column_default' => null, 'udt_name' => 'int4'], ['column_name' => 'price', 'data_type' => 'NUMERIC', 'character_maximum_length' => null, 'numeric_precision' => 10, 'numeric_scale' => 2, 'is_nullable' => 'YES', 'column_default' => null, 'udt_name' => 'numeric']]);
        $pkStmt = static::createStub(StatementInterface::class);
        $pkStmt->method('fetchAll')->willReturn([['column_name' => 'id']]);
        $uniqueStmt = static::createStub(StatementInterface::class);
        $uniqueStmt->method('fetchAll')->willReturn([]);
        $connection->method('query')->willReturnCallback(function (string $sql) use ($tablesStmt, $columnsStmt, $pkStmt, $uniqueStmt) {
            if (str_contains($sql, 'information_schema.tables')) {
                return $tablesStmt;
            }
            if (str_contains($sql, 'information_schema.columns')) {
                return $columnsStmt;
            }
            if (str_contains($sql, "constraint_type = 'PRIMARY KEY'")) {
                return $pkStmt;
            }
            if (str_contains($sql, "constraint_type = 'UNIQUE'")) {
                return $uniqueStmt;
            }
            return false;
        });
        $factory = new PgSqlSessionFactory();
        $session = $factory->create($connection, ZtdConfig::default());
        $session->enable();
        $plan = $session->rewrite('SELECT * FROM products');
        self::assertStringContainsString('"products" AS MATERIALIZED', $plan->sql());
    }
    public function testCreateWithNullParseResultStillWorks(): void
    {
        $connection = static::createStub(ConnectionInterface::class);
        $tablesStmt = static::createStub(StatementInterface::class);
        $tablesStmt->method('fetchAll')->willReturn([['table_name' => 'bad_table']]);
        $columnsStmt = static::createStub(StatementInterface::class);
        $columnsStmt->method('fetchAll')->willReturn([]);
        $connection->method('query')->willReturnCallback(function (string $sql) use ($tablesStmt, $columnsStmt) {
            if (str_contains($sql, 'information_schema.tables')) {
                return $tablesStmt;
            }
            return $columnsStmt;
        });
        $factory = new PgSqlSessionFactory();
        $session = $factory->create($connection, ZtdConfig::default());
        self::assertTrue($session->isEnabled());
    }
    public function testCreateRegistersReflectedPartialUniqueIndexes(): void
    {
        $tables = self::createStub(StatementInterface::class);
        $tables->method('fetchAll')->willReturn([['table_name' => 'users']]);
        $columns = self::createStub(StatementInterface::class);
        $columns->method('fetchAll')->willReturn([['column_name' => 'id', 'data_type' => 'integer', 'is_nullable' => 'NO', 'udt_name' => 'int4'], ['column_name' => 'email', 'data_type' => 'text', 'is_nullable' => 'NO', 'udt_name' => 'text'], ['column_name' => 'status', 'data_type' => 'text', 'is_nullable' => 'NO', 'udt_name' => 'text']]);
        $primaryKey = self::createStub(StatementInterface::class);
        $primaryKey->method('fetchAll')->willReturn([['column_name' => 'id']]);
        $unique = self::createStub(StatementInterface::class);
        $unique->method('fetchAll')->willReturn([['constraint_name' => 'users_active_email', 'column_name' => 'email', 'predicate' => "status = 'active'"]]);
        $empty = self::createStub(StatementInterface::class);
        $empty->method('fetchAll')->willReturn([]);
        $connection = self::createStub(ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function (string $sql) use ($tables, $columns, $primaryKey, $unique, $empty): StatementInterface {
            return match (true) {
                str_contains($sql, 'information_schema.tables') => $tables,
                str_contains($sql, 'information_schema.columns') => $columns,
                str_contains($sql, "constraint_type = 'PRIMARY KEY'") => $primaryKey,
                str_contains($sql, 'pg_catalog.pg_index') => $unique,
                default => $empty,
            };
        });
        $session = (new PgSqlSessionFactory())->create($connection, ZtdConfig::default());
        $plan = $session->rewrite("INSERT INTO users (id, email, status) VALUES (1, 'a@example.com', 'active') " . "ON CONFLICT (email) WHERE status = 'active' " . 'DO UPDATE SET status = EXCLUDED.status');
        self::assertStringContainsString('"__ztd_existing"."email" = "__ztd_incoming"."email"', $plan->sql());
        self::assertStringContainsString('"__ztd_existing"."status" = \'active\'', $plan->sql());
    }
}
