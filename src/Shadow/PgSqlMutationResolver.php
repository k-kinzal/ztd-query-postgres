<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Shadow;

use ZtdQuery\Exception\UnknownSchemaException;
use ZtdQuery\Exception\UnsupportedSqlException;
use ZtdQuery\Platform\Postgres\Sql\Partition\PgSqlPartitionParser;
use ZtdQuery\Platform\Postgres\Sql\PgSqlParser;
use ZtdQuery\Platform\SchemaParser;
use ZtdQuery\Rewrite\QueryKind;
use ZtdQuery\Schema\TableDefinitionRegistry;
use ZtdQuery\Shadow\Mutation\ShadowMutation;
use ZtdQuery\Shadow\ShadowStore;

/**
 * Resolves the appropriate ShadowMutation for a given PostgreSQL SQL statement.
 */
final class PgSqlMutationResolver
{
    private ShadowStore $shadowStore;
    private TableDefinitionRegistry $registry;
    private SchemaParser $schemaParser;
    private PgSqlParser $parser;
    private PgSqlPartitionParser $partitionParser;

    /**
     * Initializes the collaborators and state used by this mutation resolver.
     */
    public function __construct(
        ShadowStore $shadowStore,
        TableDefinitionRegistry $registry,
        SchemaParser $schemaParser,
        PgSqlParser $parser
    ) {
        $this->shadowStore = $shadowStore;
        $this->registry = $registry;
        $this->schemaParser = $schemaParser;
        $this->parser = $parser;
        $this->partitionParser = new PgSqlPartitionParser();
    }

    /**
     * Resolve mutation for a given SQL statement.
     *
     * @throws UnsupportedSqlException
     * @throws UnknownSchemaException
     */
    public function resolve(string $sql, string $statementType, QueryKind $kind): ?ShadowMutation
    {
        return match ($statementType) {
            'INSERT' => (new Mutation\Row\RowMutationResolver($this->parser, $this->registry, $this->shadowStore))->resolveInsert($sql),
            'UPDATE' => (new Mutation\Row\RowMutationResolver($this->parser, $this->registry, $this->shadowStore))->resolveUpdate($sql),
            'DELETE' => (new Mutation\Row\RowMutationResolver($this->parser, $this->registry, $this->shadowStore))->resolveDelete($sql),
            'MERGE' => (new Mutation\Row\RowMutationResolver($this->parser, $this->registry, $this->shadowStore))->resolveMerge($sql),
            'TRUNCATE' => (new Mutation\Row\RowMutationResolver($this->parser, $this->registry, $this->shadowStore))->resolveTruncate($sql),
            'CREATE_TABLE' => (new Mutation\Table\TableMutationResolver($this->parser, $this->partitionParser, $this->registry, $this->schemaParser))->resolveCreateTable($sql),
            'DROP_TABLE' => (new Mutation\Table\TableMutationResolver($this->parser, $this->partitionParser, $this->registry, $this->schemaParser))->resolveDropTable($sql),
            'ALTER_TABLE' => (new Mutation\Table\TableMutationResolver($this->parser, $this->partitionParser, $this->registry, $this->schemaParser))->resolveAlterTable($sql),
            default => null,
        };
    }
}
