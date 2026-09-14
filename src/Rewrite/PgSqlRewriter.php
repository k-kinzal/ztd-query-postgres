<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Rewrite;

use ZtdQuery\Exception\UnknownSchemaException;
use ZtdQuery\Exception\UnsupportedSqlException;
use ZtdQuery\Platform\Postgres\Rewrite\Cte\PgSqlCteShadowComposer;
use ZtdQuery\Platform\Postgres\Rewrite\Partition\PgSqlPartitionPredicateRenderer;
use ZtdQuery\Platform\Postgres\Rewrite\Returning\PgSqlReturningProjectionParser;
use ZtdQuery\Platform\Postgres\Rewrite\Transformer\PgSqlTransformer;
use ZtdQuery\Platform\Postgres\Shadow\PgSqlMutationResolver;
use ZtdQuery\Platform\Postgres\Sql\PgSqlParser;
use ZtdQuery\Platform\Postgres\Sql\Transaction\PgSqlTransactionStatementParser;
use ZtdQuery\Rewrite\MultiRewritePlan;
use ZtdQuery\Rewrite\RewritePlan;
use ZtdQuery\Rewrite\RewriteStateCommitter;
use ZtdQuery\Rewrite\SqlRewriter;
use ZtdQuery\Schema\TableDefinitionRegistry;
use ZtdQuery\Schema\ViewDefinitionSet;
use ZtdQuery\Shadow\ShadowStore;
use ZtdQuery\Sql\TransactionStatement;

/**
 * PostgreSQL rewrite implementation for ZTD.
 *
 * Orchestrates parsing, classification, transformation, and mutation resolution.
 * Uses Result Select Query approach (not RETURNING) for consistency across platforms.
 *
 * @visibility public
 * @example Compose a rewriter for an empty catalog
 *     $parser = new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser();
 *     $store = new \ZtdQuery\Shadow\ShadowStore();
 *     $registry = new \ZtdQuery\Schema\TableDefinitionRegistry();
 *     $select = new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\SelectTransformer();
 *     $transformer = new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\PgSqlTransformer($parser, $select, new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\InsertTransformer($parser, $select), new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\UpdateTransformer($parser, $select), new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\DeleteTransformer($parser, $select));
 *     $resolver = new \ZtdQuery\Platform\Postgres\Shadow\PgSqlMutationResolver($store, $registry, new \ZtdQuery\Platform\Postgres\Schema\PgSqlSchemaParser(), $parser);
 *     $rewriter = new \ZtdQuery\Platform\Postgres\Rewrite\PgSqlRewriter(new \ZtdQuery\Platform\Postgres\Rewrite\PgSqlQueryGuard($parser), $store, $registry, $transformer, $resolver, $parser);
 *     $rewriter->rewrite('SELECT 1')->sql() // => 'SELECT 1'
 */
final class PgSqlRewriter implements SqlRewriter, RewriteStateCommitter
{
    /**
     * Parses a transaction control statement into a shadow transaction operation.
     * @visibility public
     * @example Ignore nontransaction statements
     *     $parser = new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser();
     *     $store = new \ZtdQuery\Shadow\ShadowStore();
     *     $registry = new \ZtdQuery\Schema\TableDefinitionRegistry();
     *     $select = new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\SelectTransformer();
     *     $transformer = new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\PgSqlTransformer($parser, $select, new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\InsertTransformer($parser, $select), new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\UpdateTransformer($parser, $select), new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\DeleteTransformer($parser, $select));
     *     $resolver = new \ZtdQuery\Platform\Postgres\Shadow\PgSqlMutationResolver($store, $registry, new \ZtdQuery\Platform\Postgres\Schema\PgSqlSchemaParser(), $parser);
     *     $rewriter = new \ZtdQuery\Platform\Postgres\Rewrite\PgSqlRewriter(new \ZtdQuery\Platform\Postgres\Rewrite\PgSqlQueryGuard($parser), $store, $registry, $transformer, $resolver, $parser);
     *     $rewriter->transactionStatement('SELECT 1') // => null
     */
    public function transactionStatement(string $sql): ?TransactionStatement
    {
        return (new PgSqlTransactionStatementParser())->parse($sql);
    }

    private PgSqlQueryGuard $guard;
    private ShadowStore $shadowStore;
    private TableDefinitionRegistry $registry;
    private PgSqlTransformer $transformer;
    private PgSqlMutationResolver $mutationResolver;
    private PgSqlParser $parser;
    private PgSqlReturningProjectionParser $returningProjectionParser;
    private PgSqlCteShadowComposer $cteComposer;
    private PgSqlPartitionPredicateRenderer $partitionPredicateRenderer;
    private ViewDefinitionSet $views;

    /**
     * Initializes the collaborators and state used by this rewriter.
     */
    public function __construct(
        PgSqlQueryGuard $guard,
        ShadowStore $shadowStore,
        TableDefinitionRegistry $registry,
        PgSqlTransformer $transformer,
        PgSqlMutationResolver $mutationResolver,
        PgSqlParser $parser,
        ?ViewDefinitionSet $views = null,
    ) {
        $this->guard = $guard;
        $this->shadowStore = $shadowStore;
        $this->registry = $registry;
        $this->transformer = $transformer;
        $this->mutationResolver = $mutationResolver;
        $this->parser = $parser;
        $this->returningProjectionParser = new PgSqlReturningProjectionParser();
        $this->cteComposer = new PgSqlCteShadowComposer();
        $this->partitionPredicateRenderer = new PgSqlPartitionPredicateRenderer();
        $this->views = $views ?? new ViewDefinitionSet();
    }

    /**
     * {@inheritDoc}
     *
     * @throws UnsupportedSqlException When SQL is empty, unparseable, or multi-statement.
     * @throws UnknownSchemaException When SQL references unknown tables/columns.
     * @visibility public
     * @example Rewrite a constant read
     *     $parser = new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser();
     *     $store = new \ZtdQuery\Shadow\ShadowStore();
     *     $registry = new \ZtdQuery\Schema\TableDefinitionRegistry();
     *     $select = new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\SelectTransformer();
     *     $transformer = new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\PgSqlTransformer($parser, $select, new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\InsertTransformer($parser, $select), new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\UpdateTransformer($parser, $select), new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\DeleteTransformer($parser, $select));
     *     $resolver = new \ZtdQuery\Platform\Postgres\Shadow\PgSqlMutationResolver($store, $registry, new \ZtdQuery\Platform\Postgres\Schema\PgSqlSchemaParser(), $parser);
     *     $rewriter = new \ZtdQuery\Platform\Postgres\Rewrite\PgSqlRewriter(new \ZtdQuery\Platform\Postgres\Rewrite\PgSqlQueryGuard($parser), $store, $registry, $transformer, $resolver, $parser);
     *     $plan = $rewriter->rewrite('SELECT 1');
     *     $plan->sql() // => 'SELECT 1'
     *     $plan->kind() === \ZtdQuery\Rewrite\QueryKind::READ // => true
     */
    public function rewrite(string $sql): RewritePlan
    {
        $sql = trim($sql);
        if ($sql === '') {
            throw new UnsupportedSqlException($sql, 'Empty or unparseable');
        }

        $statements = $this->splitStatements($sql);
        if ($statements === []) {
            throw new UnsupportedSqlException($sql, 'Empty or unparseable');
        }

        if (count($statements) > 1) {
            throw new UnsupportedSqlException($sql, 'Multi-statement');
        }

        return (new StatementRewriter($this->cteComposer, $this->guard, $this->mutationResolver, $this->parser, $this->partitionPredicateRenderer, $this->registry, $this->returningProjectionParser, $this->shadowStore, $this->transformer, $this->views))->rewriteStatement($statements[0]);
    }

    /**
     * {@inheritDoc}
     *
     * @throws UnsupportedSqlException When SQL is empty or unparseable.
     * @throws UnknownSchemaException When SQL references unknown tables/columns.
     * @visibility public
     * @example Build ordered plans for multiple reads
     *     $parser = new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser();
     *     $store = new \ZtdQuery\Shadow\ShadowStore();
     *     $registry = new \ZtdQuery\Schema\TableDefinitionRegistry();
     *     $select = new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\SelectTransformer();
     *     $transformer = new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\PgSqlTransformer($parser, $select, new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\InsertTransformer($parser, $select), new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\UpdateTransformer($parser, $select), new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\DeleteTransformer($parser, $select));
     *     $resolver = new \ZtdQuery\Platform\Postgres\Shadow\PgSqlMutationResolver($store, $registry, new \ZtdQuery\Platform\Postgres\Schema\PgSqlSchemaParser(), $parser);
     *     $rewriter = new \ZtdQuery\Platform\Postgres\Rewrite\PgSqlRewriter(new \ZtdQuery\Platform\Postgres\Rewrite\PgSqlQueryGuard($parser), $store, $registry, $transformer, $resolver, $parser);
     *     count($rewriter->rewriteMultiple('SELECT 1; SELECT 2')->plans()) // => 2
     */
    public function rewriteMultiple(string $sql): MultiRewritePlan
    {
        $sql = trim($sql);
        if ($sql === '') {
            throw new UnsupportedSqlException($sql, 'Empty or unparseable');
        }

        $statements = $this->splitStatements($sql);
        if ($statements === []) {
            throw new UnsupportedSqlException($sql, 'Empty or unparseable');
        }

        $plans = [];
        foreach ($statements as $stmt) {
            $plans[] = (new StatementRewriter($this->cteComposer, $this->guard, $this->mutationResolver, $this->parser, $this->partitionPredicateRenderer, $this->registry, $this->returningProjectionParser, $this->shadowStore, $this->transformer, $this->views))->rewriteStatement($stmt);
        }

        return new MultiRewritePlan($plans);
    }

    /**
     * {@inheritDoc}
     * @visibility public
     * @example Keep statement delimiters inside strings
     *     $parser = new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser();
     *     $store = new \ZtdQuery\Shadow\ShadowStore();
     *     $registry = new \ZtdQuery\Schema\TableDefinitionRegistry();
     *     $select = new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\SelectTransformer();
     *     $transformer = new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\PgSqlTransformer($parser, $select, new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\InsertTransformer($parser, $select), new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\UpdateTransformer($parser, $select), new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\DeleteTransformer($parser, $select));
     *     $resolver = new \ZtdQuery\Platform\Postgres\Shadow\PgSqlMutationResolver($store, $registry, new \ZtdQuery\Platform\Postgres\Schema\PgSqlSchemaParser(), $parser);
     *     $rewriter = new \ZtdQuery\Platform\Postgres\Rewrite\PgSqlRewriter(new \ZtdQuery\Platform\Postgres\Rewrite\PgSqlQueryGuard($parser), $store, $registry, $transformer, $resolver, $parser);
     *     $rewriter->splitStatements("SELECT ';'; SELECT 2") // => ["SELECT ';'", 'SELECT 2']
     */
    public function splitStatements(string $sql): array
    {
        return $this->parser->splitStatements($sql);
    }

    /**
     * Commits staged generated identity values after a successful rewrite.
     * @visibility public
     * @example Commit after a successful read
     *     $parser = new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser();
     *     $store = new \ZtdQuery\Shadow\ShadowStore();
     *     $registry = new \ZtdQuery\Schema\TableDefinitionRegistry();
     *     $select = new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\SelectTransformer();
     *     $transformer = new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\PgSqlTransformer($parser, $select, new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\InsertTransformer($parser, $select), new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\UpdateTransformer($parser, $select), new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\DeleteTransformer($parser, $select));
     *     $resolver = new \ZtdQuery\Platform\Postgres\Shadow\PgSqlMutationResolver($store, $registry, new \ZtdQuery\Platform\Postgres\Schema\PgSqlSchemaParser(), $parser);
     *     $rewriter = new \ZtdQuery\Platform\Postgres\Rewrite\PgSqlRewriter(new \ZtdQuery\Platform\Postgres\Rewrite\PgSqlQueryGuard($parser), $store, $registry, $transformer, $resolver, $parser);
     *     $rewriter->rewrite('SELECT 1');
     *     $rewriter->commitRewriteState();
     *     $rewriter->rewrite('SELECT 2')->sql() // => 'SELECT 2'
     */
    public function commitRewriteState(): void
    {
        $this->transformer->commitRewriteState();
    }

    /**
     * Returns a PostgreSQL SELECT that produces no rows.
     * @visibility public
     * @example Produce an empty result query
     *     $parser = new \ZtdQuery\Platform\Postgres\Sql\PgSqlParser();
     *     $store = new \ZtdQuery\Shadow\ShadowStore();
     *     $registry = new \ZtdQuery\Schema\TableDefinitionRegistry();
     *     $select = new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\SelectTransformer();
     *     $transformer = new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\PgSqlTransformer($parser, $select, new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\InsertTransformer($parser, $select), new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\UpdateTransformer($parser, $select), new \ZtdQuery\Platform\Postgres\Rewrite\Transformer\DeleteTransformer($parser, $select));
     *     $resolver = new \ZtdQuery\Platform\Postgres\Shadow\PgSqlMutationResolver($store, $registry, new \ZtdQuery\Platform\Postgres\Schema\PgSqlSchemaParser(), $parser);
     *     $rewriter = new \ZtdQuery\Platform\Postgres\Rewrite\PgSqlRewriter(new \ZtdQuery\Platform\Postgres\Rewrite\PgSqlQueryGuard($parser), $store, $registry, $transformer, $resolver, $parser);
     *     $rewriter->emptyResultSelect() // => 'SELECT 1 WHERE FALSE'
     */
    public function emptyResultSelect(): string
    {
        return 'SELECT 1 WHERE FALSE';
    }
}
