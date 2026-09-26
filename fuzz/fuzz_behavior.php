<?php

/**
 * Compare grammar-generated SQL with native PostgreSql execution and check ZTD isolation.
 *
 * Usage: vendor/bin/php-fuzzer fuzz fuzz/fuzz_behavior.php /path/to/corpus/ --timeout=60
 * Copy sql-faker/seeds/pg/pg-17.2/* into that corpus to replay grammar seeds.
 * Default mode uses the exact sql-faker byte decoder and unconstrained statement root.
 * ZTD_FUZZ_FIXTURES=1 constrains DML table/column roles through Plan for populated fixtures;
 * use a separate corpus for this mode. SQLFAKER_COVERAGE=0 disables coverage recording.
 * Native errors are compared with ZTD rejections, never discarded by an allowlist.
 */

declare(strict_types=1);

use Container\PostgreSql17Container;
use Faker\Factory;
use Fuzz\Target\BehaviorTarget;
use SqlFaker\Generation\Choice\BytePlanCompiler;
use SqlFaker\Generation\Coverage\GrammarCoverage;
use SqlFaker\Generation\Plan\GenerationPlan;
use SqlFaker\Generation\Plan\LexemeConstraint;
use SqlFaker\Generation\Plan\ProductionPattern;
use SqlFaker\Generation\Plan\RulePlan;
use SqlFaker\PostgreSql\PostgreSqlProvider;
use Testcontainers\Testcontainers;

/**
 * Register before Testcontainers so the PHP-Fuzzer alarm cannot interrupt teardown.
 */
register_shutdown_function(static function (): void {
    if (function_exists('pcntl_alarm')) {
        pcntl_alarm(0);
    }
});
$instance = Testcontainers::run(PostgreSql17Container::class);
$host = str_replace('localhost', '127.0.0.1', $instance->getHost());
$port = $instance->getMappedPort(5432);
$target = new BehaviorTarget("pgsql:host={$host};port={$port};dbname=test");

$fixtures = getenv('ZTD_FUZZ_FIXTURES') === '1';
$coverage = getenv('SQLFAKER_COVERAGE') === '0' ? null : new GrammarCoverage(__DIR__ . '/coverage/' . ($fixtures ? 'fixtures' : 'behavior'));
$provider = new PostgreSqlProvider(Factory::create(), 'pg-17.2', $coverage);
$planner = $provider->planner();
$constraints = GenerationPlan::fromRule('stmt')->requiringNonEmpty();
if ($fixtures) {
    $constraints = $constraints
        ->withRule('stmt', RulePlan::any()->allowing(ProductionPattern::anyOf(
            ProductionPattern::exactly('SelectStmt'),
            ProductionPattern::exactly('InsertStmt'),
            ProductionPattern::exactly('UpdateStmt'),
            ProductionPattern::exactly('DeleteStmt'),
        )))
        ->withRule('ColId', RulePlan::any()->allowing(ProductionPattern::exactly('IDENT')))
        ->withRule('qualified_name', RulePlan::any()->allowing(ProductionPattern::exactly('ColId'))
            ->withLexeme('IDENT', LexemeConstraint::oneOf('items')))
        ->withRule('columnref', RulePlan::any()->allowing(ProductionPattern::exactly('ColId'))
            ->withLexeme('IDENT', LexemeConstraint::oneOf('id', 'value', 'label')));
    $constraints = $constraints->withExpansionBudget(256);
}

/**
 * @var PhpFuzzer\Config $config
 */
$config->setAllowedExceptions([]);
$config->setMaxLen(80004);
$config->setTarget(static function (string $input) use ($provider, $planner, $constraints, $target): void {
    $plan = (new BytePlanCompiler())->compile($input, $planner, $constraints);
    $target->verify($provider->generate($plan), $input);
});
