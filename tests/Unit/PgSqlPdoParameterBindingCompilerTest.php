<?php

declare(strict_types=1);

namespace Tests\Unit;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Platform\Postgres\PgSqlPdoParameterBindingCompiler;
use ZtdQuery\Platform\Postgres\PgSqlPdoPlaceholderEscaper;

#[CoversClass(PgSqlPdoParameterBindingCompiler::class)]
#[UsesClass(PgSqlPdoPlaceholderEscaper::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlLexerProfile::class)]
final class PgSqlPdoParameterBindingCompilerTest extends TestCase
{
    public function testMapsNativePositionsAndEscapesPostgreSqlOperators(): void
    {
        $compiled = (new PgSqlPdoParameterBindingCompiler())->compile(
            'SELECT $2, $1, $2, \'$3\' FROM docs WHERE meta ? $1 -- $4',
            ['first', 'second'],
        );

        self::assertSame(
            'SELECT :__ztd_pdo_2, :__ztd_pdo_1, :__ztd_pdo_2, \'$3\' FROM docs WHERE meta ?? :__ztd_pdo_1 -- $4',
            $compiled['sql'],
        );
        self::assertSame(
            ['__ztd_pdo_2' => 'second', '__ztd_pdo_1' => 'first'],
            $compiled['params'],
        );
    }

    public function testPreservesPdoPlaceholdersAndHandlesMissingNativeValues(): void
    {
        $compiler = new PgSqlPdoParameterBindingCompiler();

        self::assertSame(
            ['sql' => 'SELECT ?', 'params' => [1]],
            $compiler->compile('SELECT ?', [1]),
        );
        self::assertSame(
            ['sql' => 'SELECT $0, :__ztd_pdo_1', 'params' => ['__ztd_pdo_1' => 1]],
            $compiler->compile('SELECT $0, $1', [1]),
        );
        self::assertSame(
            ['sql' => 'SELECT :__ztd_pdo_1', 'params' => null],
            $compiler->compile('SELECT $1', null),
        );
        self::assertSame(
            ['sql' => 'SELECT ?, :__ztd_pdo_1', 'params' => ['__ztd_pdo_1' => 1]],
            $compiler->compile('SELECT ?, $1', [1]),
        );
        self::assertSame(
            ['sql' => 'SELECT $$ $1 $$, :__ztd_pdo_1', 'params' => ['__ztd_pdo_1' => 1]],
            $compiler->compile('SELECT $$ $1 $$, $1', [1]),
        );
    }
}
