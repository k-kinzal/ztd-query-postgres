<?php

declare(strict_types=1);

namespace Fuzz\Input;

use SqlFaker\Generation\Choice\BytePlanCompiler;
use SqlFaker\Generation\Choice\PlanBuilder;
use SqlFaker\Generation\Plan\GenerationPlan;
use SqlFaker\PostgreSqlProvider;

/**
 * Freezes every structural, lexical and budget decision from corpus bytes.
 */
final class SqlInput
{
    private readonly PlanBuilder $planner;

    /**
     * Reuses the pinned provider's planner across inputs without retaining rewrite state.
     */
    public function __construct(private readonly PostgreSqlProvider $provider)
    {
        $this->planner = $provider->planner();
    }

    /**
     * Uses a family selector to keep supported DML and DDL reachable alongside the full grammar.
     */
    public function generate(string $input): string
    {
        $rules = ['stmt', 'SelectStmt', 'InsertStmt', 'UpdateStmt', 'DeleteStmt', 'CreateStmt', 'AlterTableStmt', 'DropStmt', 'TruncateStmt', 'MergeStmt', 'CopyStmt', 'ViewStmt', 'DoStmt', 'CreateDomainStmt'];
        $rule = $rules[ord($input[0] ?? "\x00") % count($rules)];
        $constraints = GenerationPlan::fromRule($rule)->requiringNonEmpty()->withExpansionBudget(1000);
        $plan = (new BytePlanCompiler())->compile(substr($input, 1), $this->planner, $constraints);
        return $this->provider->generate($plan);
    }
}
