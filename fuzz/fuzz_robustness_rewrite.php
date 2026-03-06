<?php

declare(strict_types=1);

use Faker\Factory;
use Fuzz\Robustness\Target\RewriteTarget;
use SqlFaker\PostgreSqlProvider;

$faker = Factory::create();
$provider = new PostgreSqlProvider($faker);
$target = new RewriteTarget($faker, $provider);

/** @var \PhpFuzzer\Config $config */
$config->setTarget(\Closure::fromCallable($target));
