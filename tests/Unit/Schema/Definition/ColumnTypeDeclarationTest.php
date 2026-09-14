<?php

declare(strict_types=1);

namespace Tests\Unit\Schema\Definition;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Schema\Definition\ColumnTypeDeclaration::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::class)]
final class ColumnTypeDeclarationTest extends TestCase
{
    public function testExtractType(): void
    {
        self::assertSame(['type' => 'NUMERIC(10,2)[]', 'rest' => 'NOT NULL'], (new \ZtdQuery\Platform\Postgres\Schema\Definition\ColumnTypeDeclaration())->extractType('NUMERIC(10,2)[] NOT NULL'));
    }

    public function testExtractQuotedType(): void
    {
        self::assertSame(['type' => '"tenant"."PositiveValue"[]', 'rest' => ' NOT NULL'], (new \ZtdQuery\Platform\Postgres\Schema\Definition\ColumnTypeDeclaration())->extractQuotedType('"tenant"."PositiveValue"[] NOT NULL'));
    }

    public function testIsTypeIdentifier(): void
    {
        $sql = '"PositiveValue"';
        $stream = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create());
        $tokens = $stream->significantTokens();

        self::assertSame(true, \ZtdQuery\Platform\Postgres\Schema\Definition\ColumnTypeDeclaration::isTypeIdentifier($tokens[0]));
    }
    public function testTypeNameStopsBeforeConstraints(): void
    {
        $declaration = new \ZtdQuery\Platform\Postgres\Schema\Definition\ColumnTypeDeclaration();
        self::assertSame(['type' => 'DOUBLE PRECISION', 'rest' => ' NOT NULL'], $declaration->typeName('DOUBLE PRECISION NOT NULL'));
        self::assertSame(['type' => 'INTEGER', 'rest' => ' DEFAULT 1'], $declaration->typeName('INTEGER DEFAULT 1'));
        self::assertNull($declaration->typeName('()'));
    }
}
