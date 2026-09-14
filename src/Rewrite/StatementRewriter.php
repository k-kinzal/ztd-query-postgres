<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Rewrite;

use ZtdQuery\Exception\UnknownSchemaException;
use ZtdQuery\Exception\UnsupportedSqlException;
use ZtdQuery\Platform\Postgres\Rewrite\Context\TableContext as SchemaContext;
use ZtdQuery\Platform\Postgres\Rewrite\Cte\PgSqlCteShadowComposer;
use ZtdQuery\Platform\Postgres\Rewrite\Partition\PgSqlPartitionPredicateRenderer;
use ZtdQuery\Platform\Postgres\Rewrite\Returning\PgSqlReturningProjectionParser;
use ZtdQuery\Platform\Postgres\Rewrite\Transformer\PgSqlTransformer;
use ZtdQuery\Platform\Postgres\Shadow\PgSqlMutationResolver;
use ZtdQuery\Platform\Postgres\Sql\Diagnostic\PgSqlReadOnlyDiagnosticStatement;
use ZtdQuery\Platform\Postgres\Sql\PgSqlParser;
use ZtdQuery\Rewrite\AffectedRowsMode;
use ZtdQuery\Rewrite\QueryKind;
use ZtdQuery\Rewrite\RewritePlan;
use ZtdQuery\Schema\TableDefinitionRegistry;
use ZtdQuery\Schema\ViewDefinitionSet;
use ZtdQuery\Shadow\ShadowStore;

/**
 * Statement rewriter operations for PostgreSQL session.
 *
 * @visibility root
 */
final class StatementRewriter
{
    private readonly PgSqlCteShadowComposer $cteComposer;
    private readonly PgSqlQueryGuard $guard;
    private readonly PgSqlMutationResolver $mutationResolver;
    private readonly PgSqlParser $parser;
    private readonly PgSqlPartitionPredicateRenderer $partitionPredicateRenderer;
    private readonly TableDefinitionRegistry $registry;
    private readonly PgSqlReturningProjectionParser $returningProjectionParser;
    private readonly ShadowStore $shadowStore;
    private readonly PgSqlTransformer $transformer;
    private readonly ViewDefinitionSet $views;

    /**
     * Supplies the dependencies used by this StatementRewriter.
     */
    public function __construct(PgSqlCteShadowComposer $cteComposer, PgSqlQueryGuard $guard, PgSqlMutationResolver $mutationResolver, PgSqlParser $parser, PgSqlPartitionPredicateRenderer $partitionPredicateRenderer, TableDefinitionRegistry $registry, PgSqlReturningProjectionParser $returningProjectionParser, ShadowStore $shadowStore, PgSqlTransformer $transformer, ViewDefinitionSet $views)
    {
        $this->cteComposer = $cteComposer;
        $this->guard = $guard;
        $this->mutationResolver = $mutationResolver;
        $this->parser = $parser;
        $this->partitionPredicateRenderer = $partitionPredicateRenderer;
        $this->registry = $registry;
        $this->returningProjectionParser = $returningProjectionParser;
        $this->shadowStore = $shadowStore;
        $this->transformer = $transformer;
        $this->views = $views;
    }

    /**
     * Rewrite statement.
     * @throws UnknownSchemaException
     * @throws UnsupportedSqlException
     */
    public function rewriteStatement(string $sql): RewritePlan
    {
        if (PgSqlReadOnlyDiagnosticStatement::isSafe($sql)) {
            return new RewritePlan($sql, QueryKind::READ);
        }
        $kind = $this->guard->classify($sql);
        if ($kind === null) {
            throw new UnsupportedSqlException($sql, 'Statement type not supported');
        }

        if ($kind === QueryKind::SKIPPED) {
            return new RewritePlan($sql, QueryKind::SKIPPED);
        }

        $statementType = $this->parser->classifyStatement($sql);
        if ($statementType === 'DO') {
            return new RewritePlan($sql, QueryKind::READ);
        }
        $tableContext = (new SchemaContext($this->partitionPredicateRenderer, $this->registry, $this->shadowStore, $this->views))->buildTableContext($this->shadowStore->getAll());

        if ($kind === QueryKind::READ) {
            $this->validateReadTables($sql);

            $transformedSql = $this->transformer->transform($sql, $tableContext);

            return new RewritePlan($transformedSql, QueryKind::READ);
        }

        return $this->rewriteMutation($sql, $kind, $statementType, $tableContext);
    }

    /**
     * Rejects unknown physical relations while allowing CTE names declared by the statement.
     * @throws UnknownSchemaException
     */
    public function validateReadTables(string $sql): void
    {
        if ((new SchemaContext($this->partitionPredicateRenderer, $this->registry, $this->shadowStore, $this->views))->hasSchemaContext()) {
            $tableNames = $this->parser->extractSelectTableNames($sql);
            $declaredCtes = array_fill_keys($this->cteComposer->declaredCteNames($sql), true);
            foreach ($tableNames as $tableName) {
                if (isset($declaredCtes[strtolower($tableName)])) {
                    continue;
                }
                if (!(new SchemaContext($this->partitionPredicateRenderer, $this->registry, $this->shadowStore, $this->views))->tableExists($tableName)) {
                    throw new UnknownSchemaException($sql, $tableName, 'table');
                }
            }
        }

    }

    /**
     * Constructs the DDL or DML result-select plan and its simulated mutation.
     * @template T
     * @param array<string, array{viewSql: string}|array{rows: array<int, array<string, T>>, columns: array<int, string>, columnTypes: array<string, \ZtdQuery\Schema\ColumnDeclaration>, columnDefaults: array<string, string>, identityStrategies: array<string, \ZtdQuery\Schema\Key\IdentityGenerationStrategy>, generatedExpressions: array<string, string>, sourceSql?: string, storageTable?: string}> $tableContext
     */
    public function rewriteMutation(string $sql, QueryKind $kind, ?string $statementType, array $tableContext): RewritePlan
    {
        if ($kind === QueryKind::DDL_SIMULATED) {
            $mutation = $this->mutationResolver->resolve($sql, $statementType ?? '', $kind);

            if ($statementType === 'CREATE_TABLE' && $this->parser->hasCreateTableAsSelect($sql)) {
                $selectSql = $this->parser->extractCreateTableSelectSql($sql);
                if ($selectSql !== null) {
                    $transformedSelectSql = $this->transformer->transform($selectSql, $tableContext);

                    return new RewritePlan($transformedSelectSql, QueryKind::DDL_SIMULATED, $mutation);
                }
            }

            return new RewritePlan('SELECT 1 WHERE FALSE', QueryKind::DDL_SIMULATED, $mutation);
        }

        $mutation = $this->mutationResolver->resolve($sql, $statementType ?? '', $kind);

        if ($statementType === 'TRUNCATE') {
            return new RewritePlan('SELECT 1 WHERE FALSE', QueryKind::WRITE_SIMULATED, $mutation);
        }

        $transformedSql = $this->transformer->transform($sql, $tableContext);

        return new RewritePlan(
            $transformedSql,
            QueryKind::WRITE_SIMULATED,
            $mutation,
            $this->returningProjectionParser->parse($sql),
            $statementType === 'MERGE' ? AffectedRowsMode::Changed : AffectedRowsMode::Matched,
        );
    }
}
