<?php

declare(strict_types=1);

use Faker\Factory;
use Fuzz\Input\SqlInput;
use Fuzz\RewriteCheck;
use SqlFaker\Generation\Coverage\GrammarCoverage;
use SqlFaker\PostgreSqlProvider;

register_shutdown_function(static function (): void {
    if (function_exists('pcntl_alarm')) {
        pcntl_alarm(0);
    }
});

$coverage = getenv('SQLFAKER_COVERAGE') === '0' ? null : new GrammarCoverage(__DIR__ . '/coverage/full');
$input = new SqlInput(new PostgreSqlProvider(Factory::create(), 'pg-17.2', $coverage));
/** @var PhpFuzzer\Config $config */
$config->setAllowedExceptions([]);
$config->setMaxLen(20005);
$config->setTarget(static function (string $bytes) use ($input): void {
    $sql = $input->generate($bytes);
    (new RewriteCheck())->verify($sql, true);
});
