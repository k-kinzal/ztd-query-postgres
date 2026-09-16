<?php

declare(strict_types=1);

use Composer\Autoload\ClassLoader;

/**
 * Former public names of classes moved into responsibility namespaces.
 *
 * A former name becomes an alias as soon as either name is autoloaded, so a
 * parameter or return type declared with a former name accepts the relocated
 * class while no implementation is loaded during Composer bootstrap.
 *
 * @var array<class-string, string>
 */
$formerNames = [
    ZtdQuery\Platform\Postgres\Connection\Copy\PgSqlCopySupport::class => 'ZtdQuery\\Platform\\Postgres\\PgSqlCopySupport',
    ZtdQuery\Platform\Postgres\Connection\PgSqlErrorClassifier::class => 'ZtdQuery\\Platform\\Postgres\\PgSqlErrorClassifier',
    ZtdQuery\Platform\Postgres\Connection\Parameter\PgSqlPdoParameterBindingCompiler::class => 'ZtdQuery\\Platform\\Postgres\\PgSqlPdoParameterBindingCompiler',
    ZtdQuery\Platform\Postgres\Connection\Parameter\PgSqlPdoPlaceholderEscaper::class => 'ZtdQuery\\Platform\\Postgres\\PgSqlPdoPlaceholderEscaper',
    ZtdQuery\Platform\Postgres\Connection\Result\PgSqlPdoResultColumnTypeResolver::class => 'ZtdQuery\\Platform\\Postgres\\PgSqlPdoResultColumnTypeResolver',
    ZtdQuery\Platform\Postgres\Rewrite\Cte\PgSqlCteShadowComposer::class => 'ZtdQuery\\Platform\\Postgres\\PgSqlCteShadowComposer',
    ZtdQuery\Platform\Postgres\Rewrite\GeneratedColumn\PgSqlGeneratedColumnProjector::class => 'ZtdQuery\\Platform\\Postgres\\PgSqlGeneratedColumnProjector',
    ZtdQuery\Platform\Postgres\Rewrite\Partition\PgSqlPartitionPredicateRenderer::class => 'ZtdQuery\\Platform\\Postgres\\PgSqlPartitionPredicateRenderer',
    ZtdQuery\Platform\Postgres\Rewrite\PgSqlQueryGuard::class => 'ZtdQuery\\Platform\\Postgres\\PgSqlQueryGuard',
    ZtdQuery\Platform\Postgres\Rewrite\PgSqlRewriter::class => 'ZtdQuery\\Platform\\Postgres\\PgSqlRewriter',
    ZtdQuery\Platform\Postgres\Rewrite\Returning\PgSqlReturningProjectionParser::class => 'ZtdQuery\\Platform\\Postgres\\PgSqlReturningProjectionParser',
    ZtdQuery\Platform\Postgres\Rewrite\Sampling\PgSqlTableSampleRewriter::class => 'ZtdQuery\\Platform\\Postgres\\PgSqlTableSampleRewriter',
    ZtdQuery\Platform\Postgres\Rewrite\Transformer\DeleteTransformer::class => 'ZtdQuery\\Platform\\Postgres\\Transformer\\DeleteTransformer',
    ZtdQuery\Platform\Postgres\Rewrite\Transformer\InsertRowRenderer::class => 'ZtdQuery\\Platform\\Postgres\\Transformer\\InsertRowRenderer',
    ZtdQuery\Platform\Postgres\Rewrite\Transformer\InsertSelectRenderer::class => 'ZtdQuery\\Platform\\Postgres\\Transformer\\InsertSelectRenderer',
    ZtdQuery\Platform\Postgres\Rewrite\Transformer\InsertTransformer::class => 'ZtdQuery\\Platform\\Postgres\\Transformer\\InsertTransformer',
    ZtdQuery\Platform\Postgres\Rewrite\Transformer\MergeTransformer::class => 'ZtdQuery\\Platform\\Postgres\\Transformer\\MergeTransformer',
    ZtdQuery\Platform\Postgres\Rewrite\Transformer\PgSqlTransformer::class => 'ZtdQuery\\Platform\\Postgres\\PgSqlTransformer',
    ZtdQuery\Platform\Postgres\Rewrite\Transformer\SelectTransformer::class => 'ZtdQuery\\Platform\\Postgres\\Transformer\\SelectTransformer',
    ZtdQuery\Platform\Postgres\Rewrite\Transformer\UpdateTransformer::class => 'ZtdQuery\\Platform\\Postgres\\Transformer\\UpdateTransformer',
    ZtdQuery\Platform\Postgres\Rewrite\Upsert\PgSqlNativeUpsertProjector::class => 'ZtdQuery\\Platform\\Postgres\\PgSqlNativeUpsertProjector',
    ZtdQuery\Platform\Postgres\Rewrite\View\PgSqlViewShadowRenderer::class => 'ZtdQuery\\Platform\\Postgres\\PgSqlViewShadowRenderer',
    ZtdQuery\Platform\Postgres\Schema\Key\PgSqlForeignKeyDefinitionParser::class => 'ZtdQuery\\Platform\\Postgres\\PgSqlForeignKeyDefinitionParser',
    ZtdQuery\Platform\Postgres\Schema\Partition\PgSqlPartitionReflector::class => 'ZtdQuery\\Platform\\Postgres\\PgSqlPartitionReflector',
    ZtdQuery\Platform\Postgres\Schema\PgSqlColumnTypeMapper::class => 'ZtdQuery\\Platform\\Postgres\\PgSqlColumnTypeMapper',
    ZtdQuery\Platform\Postgres\Schema\PgSqlSchemaParser::class => 'ZtdQuery\\Platform\\Postgres\\PgSqlSchemaParser',
    ZtdQuery\Platform\Postgres\Schema\PgSqlSchemaReflector::class => 'ZtdQuery\\Platform\\Postgres\\PgSqlSchemaReflector',
    ZtdQuery\Platform\Postgres\Schema\View\PgSqlViewDefinitionParser::class => 'ZtdQuery\\Platform\\Postgres\\PgSqlViewDefinitionParser',
    ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\PgSqlUpsertExpressionParser::class => 'ZtdQuery\\Platform\\Postgres\\PgSqlUpsertExpressionParser',
    ZtdQuery\Platform\Postgres\Shadow\PgSqlMutationResolver::class => 'ZtdQuery\\Platform\\Postgres\\PgSqlMutationResolver',
    ZtdQuery\Platform\Postgres\Sql\Conflict\PgSqlConflictTarget::class => 'ZtdQuery\\Platform\\Postgres\\PgSqlConflictTarget',
    ZtdQuery\Platform\Postgres\Sql\Diagnostic\PgSqlReadOnlyDiagnosticStatement::class => 'ZtdQuery\\Platform\\Postgres\\PgSqlReadOnlyDiagnosticStatement',
    ZtdQuery\Platform\Postgres\Sql\Merge\PgSqlMergeActionKind::class => 'ZtdQuery\\Platform\\Postgres\\PgSqlMergeActionKind',
    ZtdQuery\Platform\Postgres\Sql\Merge\PgSqlMergeClause::class => 'ZtdQuery\\Platform\\Postgres\\PgSqlMergeClause',
    ZtdQuery\Platform\Postgres\Sql\Merge\PgSqlMergeMatchKind::class => 'ZtdQuery\\Platform\\Postgres\\PgSqlMergeMatchKind',
    ZtdQuery\Platform\Postgres\Sql\Merge\PgSqlMergeParser::class => 'ZtdQuery\\Platform\\Postgres\\PgSqlMergeParser',
    ZtdQuery\Platform\Postgres\Sql\Merge\PgSqlMergeStatement::class => 'ZtdQuery\\Platform\\Postgres\\PgSqlMergeStatement',
    ZtdQuery\Platform\Postgres\Sql\Partition\PgSqlPartitionParser::class => 'ZtdQuery\\Platform\\Postgres\\PgSqlPartitionParser',
    ZtdQuery\Platform\Postgres\Sql\PgSqlIdentifierQuoter::class => 'ZtdQuery\\Platform\\Postgres\\PgSqlIdentifierQuoter',
    ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::class => 'ZtdQuery\\Platform\\Postgres\\PgSqlLexerProfile',
    ZtdQuery\Platform\Postgres\Sql\PgSqlParser::class => 'ZtdQuery\\Platform\\Postgres\\PgSqlParser',
    ZtdQuery\Platform\Postgres\Sql\PostgreSqlLexicalMasker::class => 'ZtdQuery\\Platform\\Postgres\\PostgreSqlLexicalMasker',
    ZtdQuery\Platform\Postgres\Sql\Relation\PgSqlSelectRelationParser::class => 'ZtdQuery\\Platform\\Postgres\\PgSqlSelectRelationParser',
    ZtdQuery\Platform\Postgres\Sql\Sampling\PgSqlTableSample::class => 'ZtdQuery\\Platform\\Postgres\\PgSqlTableSample',
    ZtdQuery\Platform\Postgres\Sql\Sampling\PgSqlTableSampleMethod::class => 'ZtdQuery\\Platform\\Postgres\\PgSqlTableSampleMethod',
    ZtdQuery\Platform\Postgres\Sql\Sampling\PgSqlTableSampleParser::class => 'ZtdQuery\\Platform\\Postgres\\PgSqlTableSampleParser',
    ZtdQuery\Platform\Postgres\Sql\Transaction\PgSqlTransactionStatementParser::class => 'ZtdQuery\\Platform\\Postgres\\PgSqlTransactionStatementParser',
    ZtdQuery\Platform\Postgres\Sql\Value\PgSqlCastRenderer::class => 'ZtdQuery\\Platform\\Postgres\\PgSqlCastRenderer',
    ZtdQuery\Platform\Postgres\Sql\Value\PgSqlValueRenderer::class => 'ZtdQuery\\Platform\\Postgres\\PgSqlValueRenderer',
];
$currentNames = array_flip($formerNames);

spl_autoload_register(static function (string $class) use ($formerNames, $currentNames): void {
    $current = $currentNames[$class] ?? $class;
    $former = $formerNames[$current] ?? null;
    if ($former === null) {
        return;
    }
    if (!class_exists($current, false)) {
        foreach (ClassLoader::getRegisteredLoaders() as $loader) {
            if ($loader->loadClass($current) === true) {
                break;
            }
        }
    }
    if (class_exists($current, false) && !class_exists($former, false)) {
        class_alias($current, $former);
    }
}, true, true);
