<?php

declare(strict_types=1);

namespace Fuzz\Robustness\Invariant;

use Throwable;
use ZtdQuery\Platform\Postgres\PgSqlLexerProfile;
use ZtdQuery\Rewrite\SqlRewriter;
use ZtdQuery\Shadow\Mutation\MultiTruncateMutation;
use ZtdQuery\Sql\SqlTokenStream;

final class TruncateTargetConsistencyChecker implements InvariantChecker
{
    public function __construct(
        private readonly SqlRewriter $rewriter,
    ) {
    }

    public function check(string $sql): ?InvariantViolation
    {
        $expectedCount = $this->targetCount($sql);
        if ($expectedCount === null || $expectedCount < 2) {
            return null;
        }

        try {
            $mutation = $this->rewriter->rewrite($sql)->mutation();
        } catch (Throwable) {
            return null;
        }

        if (!$mutation instanceof MultiTruncateMutation) {
            return new InvariantViolation(
                'PG-TRUNCATE-TARGETS',
                'multi-table TRUNCATE did not produce a multi-target mutation',
                $sql,
                ['expected_targets' => $expectedCount],
            );
        }

        $actualCount = count($mutation->tableNames());
        if ($actualCount !== $expectedCount) {
            return new InvariantViolation(
                'PG-TRUNCATE-TARGETS',
                'TRUNCATE mutation target count differs from the SQL target list',
                $sql,
                ['expected_targets' => $expectedCount, 'actual_targets' => $actualCount],
            );
        }

        return null;
    }

    private function targetCount(string $sql): ?int
    {
        $stream = SqlTokenStream::tokenize($sql, PgSqlLexerProfile::create());
        if ($stream->firstTopLevelKeyword() !== 'TRUNCATE') {
            return null;
        }

        $targetList = $stream->topLevelClause(
            ['TRUNCATE'],
            [['RESTART', 'IDENTITY'], ['CONTINUE', 'IDENTITY'], ['CASCADE'], ['RESTRICT']],
        );
        if ($targetList === null || $targetList === '') {
            return 0;
        }

        return count(SqlTokenStream::tokenize($targetList, PgSqlLexerProfile::create())->splitTopLevel());
    }
}
