<?php

declare(strict_types=1);

namespace Tests\Unit\Rewrite\Upsert;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Rewrite\Upsert\ExpressionBinder::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlIdentifierQuoter::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Rewrite\Upsert\ConflictPredicate::class)]
final class ExpressionBinderTest extends TestCase
{
    public function testBindExpression(): void
    {
        self::assertSame('"__ztd_incoming"."name" || "__ztd_existing"."name"', (new \ZtdQuery\Platform\Postgres\Rewrite\Upsert\ExpressionBinder(['EXCLUDED'], new \ZtdQuery\Platform\Postgres\Sql\PgSqlIdentifierQuoter()))->bindExpression('excluded.name || users.name', 'users', ['id', 'name']));

        self::assertSame('(SELECT name FROM other) || "__ztd_existing"."name"', (new \ZtdQuery\Platform\Postgres\Rewrite\Upsert\ExpressionBinder(['EXCLUDED'], new \ZtdQuery\Platform\Postgres\Sql\PgSqlIdentifierQuoter()))->bindExpression('(SELECT name FROM other) || name', 'users', ['id', 'name']));

        self::assertSame('coalesce("__ztd_existing"."name", "__ztd_incoming"."name")', (new \ZtdQuery\Platform\Postgres\Rewrite\Upsert\ExpressionBinder(['EXCLUDED'], new \ZtdQuery\Platform\Postgres\Sql\PgSqlIdentifierQuoter()))->bindExpression('coalesce(name, excluded.name)', 'users', ['id', 'name']));
    }

    public function testSubqueryTokenIndexes(): void
    {
        $sql = 'name + (SELECT id FROM other)';
        $tokens = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create())->significantTokens();

        self::assertSame([3 => true, 4 => true, 5 => true, 6 => true], (new \ZtdQuery\Platform\Postgres\Rewrite\Upsert\ExpressionBinder(['EXCLUDED'], new \ZtdQuery\Platform\Postgres\Sql\PgSqlIdentifierQuoter()))->subqueryTokenIndexes($tokens));
    }

    public function testIsIdentifier(): void
    {
        $sql = 'name';
        $tokens = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create())->significantTokens();

        self::assertSame(true, (new \ZtdQuery\Platform\Postgres\Rewrite\Upsert\ExpressionBinder(['EXCLUDED'], new \ZtdQuery\Platform\Postgres\Sql\PgSqlIdentifierQuoter()))->isIdentifier($tokens[0]));

        $sql = '"Name"';
        $tokens = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create())->significantTokens();

        self::assertSame(true, (new \ZtdQuery\Platform\Postgres\Rewrite\Upsert\ExpressionBinder(['EXCLUDED'], new \ZtdQuery\Platform\Postgres\Sql\PgSqlIdentifierQuoter()))->isIdentifier($tokens[0]));

        $sql = '(';
        $tokens = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create())->significantTokens();

        self::assertSame(false, (new \ZtdQuery\Platform\Postgres\Rewrite\Upsert\ExpressionBinder(['EXCLUDED'], new \ZtdQuery\Platform\Postgres\Sql\PgSqlIdentifierQuoter()))->isIdentifier($tokens[0]));
    }

    public function testIdentifier(): void
    {
        $sql = '"odd""name"';
        $tokens = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create())->significantTokens();

        self::assertSame('odd"name', (new \ZtdQuery\Platform\Postgres\Rewrite\Upsert\ExpressionBinder(['EXCLUDED'], new \ZtdQuery\Platform\Postgres\Sql\PgSqlIdentifierQuoter()))->identifier($tokens[0]));
    }
    public function testQualifiedReplacementBindsOnlyRecognizedNamespaces(): void
    {
        $sql = 'EXCLUDED.name';
        $tokens = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create())->significantTokens();
        $binder = new \ZtdQuery\Platform\Postgres\Rewrite\Upsert\ExpressionBinder(['EXCLUDED'], new \ZtdQuery\Platform\Postgres\Sql\PgSqlIdentifierQuoter());
        self::assertSame(['offset' => 0, 'length' => 13, 'value' => '"__ztd_incoming"."name"'], $binder->qualifiedReplacement($tokens[0], $tokens[2], 'EXCLUDED', 'users', ['excluded' => true]));
        self::assertNull($binder->qualifiedReplacement($tokens[0], $tokens[2], 'other', 'users', ['excluded' => true]));
    }

    public function testApplyReplacementsPreservesEarlierOffsets(): void
    {
        $binder = new \ZtdQuery\Platform\Postgres\Rewrite\Upsert\ExpressionBinder(['EXCLUDED'], new \ZtdQuery\Platform\Postgres\Sql\PgSqlIdentifierQuoter());
        self::assertSame('left+right', $binder->applyReplacements('a+b', [['offset' => 0, 'length' => 1, 'value' => 'left'], ['offset' => 2, 'length' => 1, 'value' => 'right']]));
    }
}
