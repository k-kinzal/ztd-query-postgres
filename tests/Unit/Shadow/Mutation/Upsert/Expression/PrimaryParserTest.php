<?php

declare(strict_types=1);

namespace Tests\Unit\Shadow\Mutation\Upsert\Expression;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\PrimaryParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\ComparisonOperator::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\ExpressionCursor::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\ExpressionLexeme::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\PrecedenceParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::class)]
final class PrimaryParserTest extends TestCase
{
    public function testParsePrimaryEvaluatesAParenthesizedExpression(): void
    {
        $sql = '(2 + 3)';
        $tokens = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create())->significantTokens();
        $cursor = new \ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\ExpressionCursor($sql, 'users', $tokens);
        $expression = (new \ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\PrimaryParser($cursor))->parsePrimary();
        self::assertSame(5, $expression->evaluate([], [], 'users'));
        self::assertSame(count($tokens), $cursor->index);
    }

    public function testColumnSourceResolvesIncomingAndExistingQualifiers(): void
    {
        $sql = 'id';
        $tokens = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create())->significantTokens();
        $cursor = new \ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\ExpressionCursor($sql, 'users', $tokens);
        $parser = new \ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\PrimaryParser($cursor);
        self::assertSame(\ZtdQuery\Shadow\Mutation\UpsertColumnSource::Incoming, $parser->columnSource('excluded'));
        self::assertSame(\ZtdQuery\Shadow\Mutation\UpsertColumnSource::Existing, $parser->columnSource('USERS'));
    }

    public function testColumnReadsTheIncomingRow(): void
    {
        $sql = 'EXCLUDED.name';
        $tokens = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create())->significantTokens();
        $cursor = new \ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\ExpressionCursor($sql, 'users', $tokens);
        $expression = (new \ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\PrimaryParser($cursor))->column($tokens[0]);
        self::assertSame('incoming', $expression->evaluate(['name' => 'existing'], ['name' => 'incoming'], 'users'));
        self::assertSame(count($tokens), $cursor->index);
    }

    public function testColumnSourceRejectsUnknownQualifiers(): void
    {
        $sql = 'other.id';
        $tokens = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create())->significantTokens();
        $cursor = new \ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\ExpressionCursor($sql, 'users', $tokens);
        $this->expectException(\ZtdQuery\Exception\UnsupportedSqlException::class);
        (new \ZtdQuery\Platform\Postgres\Shadow\Mutation\Upsert\Expression\PrimaryParser($cursor))->columnSource('other');
    }
}
