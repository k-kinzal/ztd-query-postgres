<?php

declare(strict_types=1);

namespace Bench;

use PhpBench\Attributes as Bench;
use ZtdQuery\Platform\Postgres\Sql\PgSqlParser;

/**
 * Measures PostgreSQL statement classification and splitting.
 */
final class ParserBench
{
    private PgSqlParser $parser;

    private string $selectSql = 'SELECT u.id, u.name, o.status FROM users u JOIN orders o ON o.user_id = u.id WHERE u.id = 1';

    private string $insertSql = "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')";

    /**
     * Initializes the parser before measuring SQL operations.
     */
    public function setUp(): void
    {
        $this->parser = new PgSqlParser();
    }

    /**
     * Classifies a SELECT query with a join and a filter.
     */
    #[Bench\BeforeMethods('setUp')]
    #[Bench\Revs(250)]
    public function benchClassifySelect(): void
    {
        $this->parser->classifyStatement($this->selectSql);
    }

    /**
     * Splits a single INSERT statement into parser input statements.
     */
    #[Bench\BeforeMethods('setUp')]
    #[Bench\Revs(250)]
    public function benchSplitInsert(): void
    {
        $this->parser->splitStatements($this->insertSql);
    }
}
