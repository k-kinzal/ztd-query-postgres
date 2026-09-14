<?php

declare(strict_types=1);

use Faker\Factory;
use Fuzz\Input\SqlInput;
use SqlFaker\Generation\Coverage\GrammarCoverage;
use SqlFaker\PostgreSqlProvider;
use ZtdQuery\Platform\Postgres\Rewrite\PgSqlQueryGuard;
use ZtdQuery\Platform\Postgres\Sql\PgSqlParser;

register_shutdown_function(static function (): void {
    if (function_exists('pcntl_alarm')) {
        pcntl_alarm(0);
    }
});

$coverage = getenv('SQLFAKER_COVERAGE') === '0' ? null : new GrammarCoverage(__DIR__ . '/coverage/classify');
$input = new SqlInput(new PostgreSqlProvider(Factory::create(), 'pg-17.2', $coverage));
/** @var PhpFuzzer\Config $config */
$config->setAllowedExceptions([]);
$config->setMaxLen(20005);
$config->setTarget(static function (string $bytes) use ($input): void {
    $sql = $input->generate($bytes);
    $guard = new PgSqlQueryGuard(new PgSqlParser());
    $first = $guard->classify($sql);
    if ($first !== $guard->classify($sql)) {
        throw new Error('Classification changed for the same SQL.');
    }
});
