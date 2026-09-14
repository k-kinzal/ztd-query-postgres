<?php

declare(strict_types=1);

namespace Bench;

use PhpBench\Attributes as Bench;
use ZtdQuery\Platform\Postgres\Sql\PgSqlParser;

final class ParserBench
{
    private PgSqlParser $parser;

    private string $selectSql = 'SELECT u.id, u.name, o.status FROM users u JOIN orders o ON o.user_id = u.id WHERE u.id = 1';

    private string $insertSql = "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')";

    public function setUp(): void
    {
        $this->parser = new PgSqlParser();
    }

    #[Bench\BeforeMethods('setUp')]
    #[Bench\Revs(250)]
    public function benchClassifySelect(): void
    {
        $this->parser->classifyStatement($this->selectSql);
    }

    #[Bench\BeforeMethods('setUp')]
    #[Bench\Revs(250)]
    public function benchSplitInsert(): void
    {
        $this->parser->splitStatements($this->insertSql);
    }
}
