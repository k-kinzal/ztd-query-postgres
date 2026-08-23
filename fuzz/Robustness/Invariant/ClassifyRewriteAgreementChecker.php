<?php

declare(strict_types=1);

namespace Fuzz\Robustness\Invariant;

use Throwable;
use ZtdQuery\Exception\UnknownSchemaException;
use ZtdQuery\Exception\UnsupportedSqlException;
use ZtdQuery\Platform\Postgres\PgSqlQueryGuard;
use ZtdQuery\Rewrite\QueryKind;
use ZtdQuery\Rewrite\SqlRewriter;
use ZtdQuery\Platform\Postgres\PgSqlReadOnlyDiagnosticStatement;

final class ClassifyRewriteAgreementChecker implements InvariantChecker
{
    private PgSqlQueryGuard $guard;
    private SqlRewriter $rewriter;

    public function __construct(PgSqlQueryGuard $guard, SqlRewriter $rewriter)
    {
        $this->guard = $guard;
        $this->rewriter = $rewriter;
    }

    public function check(string $sql): ?InvariantViolation
    {
        $diagnostic = PgSqlReadOnlyDiagnosticStatement::isSafe($sql);

        try {
            $classifyResult = $this->guard->classify($sql);
        } catch (Throwable $exception) {
            if ($diagnostic) {
                return new InvariantViolation('INV-L2-06', 'read-only diagnostic classification threw', $sql, ['exception' => $exception::class]);
            }

            return null;
        }

        if ($diagnostic && $classifyResult !== QueryKind::READ) {
            return new InvariantViolation('INV-L2-06', 'read-only diagnostic was not classified as READ', $sql);
        }

        if ($classifyResult === null) {
            try {
                $this->rewriter->rewrite($sql);
                return null;
            } catch (UnsupportedSqlException) {
                return null;
            } catch (Throwable) {
                return null;
            }
        }

        try {
            $plan = $this->rewriter->rewrite($sql);
        } catch (UnknownSchemaException $exception) {
            if ($diagnostic) {
                return new InvariantViolation('INV-L2-06', 'read-only diagnostic required schema metadata', $sql, ['exception' => $exception::class]);
            }

            return null;
        } catch (UnsupportedSqlException $exception) {
            if ($diagnostic) {
                return new InvariantViolation('INV-L2-06', 'read-only diagnostic was rejected', $sql, ['exception' => $exception::class]);
            }

            return null;
        } catch (Throwable $exception) {
            if ($diagnostic) {
                return new InvariantViolation('INV-L2-06', 'read-only diagnostic rewrite threw', $sql, ['exception' => $exception::class]);
            }

            return null;
        }

        if ($diagnostic && ($plan->kind() !== QueryKind::READ || $plan->sql() !== $sql)) {
            return new InvariantViolation('INV-L2-06', 'read-only diagnostic was not preserved as an unchanged READ plan', $sql);
        }

        if ($plan->kind() !== $classifyResult) {
            return new InvariantViolation(
                'INV-L2-05',
                'classify() and rewrite() disagree on QueryKind',
                $sql,
                [
                    'classify_result' => $classifyResult->value,
                    'rewrite_kind' => $plan->kind()->value,
                ]
            );
        }

        return null;
    }
}
