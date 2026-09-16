<?php

declare(strict_types=1);

namespace Tests\Unit\Sql\Statement;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Sql\Statement\ConflictClause::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Conflict\ColumnSet::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Lexing\IdentifierDecoder::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Statement\TargetTableParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Dml\Update\UpdateClauseParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Dml\Delete\DeleteClauseParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Dml\Insert\InsertClauseParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Conflict\PgSqlConflictTarget::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PostgreSqlLexicalMasker::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Lexing\CommentSpan::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Lexing\QuotedSpan::class)]
final class ConflictClauseTest extends TestCase
{
    public function testExtractOnConflictTarget(): void
    {
        $sql = 'INSERT INTO users VALUES (1) ON CONFLICT (id) DO UPDATE SET name = excluded.name WHERE users.active';
        $stream = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create());
        $tokens = $stream->significantTokens();

        self::assertEquals(new \ZtdQuery\Platform\Postgres\Sql\Conflict\PgSqlConflictTarget(specified: true, columns: ['id'], predicate: null, constraint: null), (new \ZtdQuery\Platform\Postgres\Sql\Statement\ConflictClause())->extractOnConflictTarget($sql));
    }

    public function testExtractOnConflictUpdateColumns(): void
    {
        $sql = 'INSERT INTO users VALUES (1) ON CONFLICT (id) DO UPDATE SET name = excluded.name WHERE users.active';
        $stream = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create());
        $tokens = $stream->significantTokens();

        self::assertSame(['columns' => ['name'], 'values' => ['name' => 'excluded.name']], (new \ZtdQuery\Platform\Postgres\Sql\Statement\ConflictClause())->extractOnConflictUpdateColumns($sql));
    }

    public function testExtractOnConflictUpdateWhere(): void
    {
        $sql = 'INSERT INTO users VALUES (1) ON CONFLICT (id) DO UPDATE SET name = excluded.name WHERE users.active';
        $stream = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create());
        $tokens = $stream->significantTokens();

        self::assertSame('users.active', (new \ZtdQuery\Platform\Postgres\Sql\Statement\ConflictClause())->extractOnConflictUpdateWhere($sql));
    }

    public function testFindOnConflictTargetStart(): void
    {
        $sql = 'INSERT INTO users VALUES (1) ON CONFLICT (id) DO UPDATE SET name = excluded.name WHERE users.active';
        $stream = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create());
        $tokens = $stream->significantTokens();

        self::assertSame(9, \ZtdQuery\Platform\Postgres\Sql\Statement\ConflictClause::findOnConflictTargetStart($tokens));
    }

    public function testFindTopLevelSymbol(): void
    {
        $sql = 'INSERT INTO users VALUES (1) ON CONFLICT (id) DO UPDATE SET name = excluded.name WHERE users.active';
        $stream = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create());
        $tokens = $stream->significantTokens();

        self::assertSame(11, \ZtdQuery\Platform\Postgres\Sql\Statement\ConflictClause::findTopLevelSymbol($tokens, 10, ')'));
    }

    public function testFindTopLevelKeyword(): void
    {
        $sql = 'INSERT INTO users VALUES (1) ON CONFLICT (id) DO UPDATE SET name = excluded.name WHERE users.active';
        $stream = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create());
        $tokens = $stream->significantTokens();

        self::assertSame(12, \ZtdQuery\Platform\Postgres\Sql\Statement\ConflictClause::findTopLevelKeyword($tokens, 0, 'DO'));
    }
    public function testNamedConstraintReadsTheArbiterName(): void
    {
        $sql = 'ON CONSTRAINT users_pkey DO NOTHING';
        $tokens = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create())->significantTokens();
        $target = (new \ZtdQuery\Platform\Postgres\Sql\Statement\ConflictClause())->namedConstraint($tokens, 0);
        self::assertNotNull($target);
        self::assertSame('users_pkey', $target->constraint);
        self::assertTrue($target->specified);
    }

    public function testIndexColumnsRejectsExpressionsAndPreservesNames(): void
    {
        $clause = new \ZtdQuery\Platform\Postgres\Sql\Statement\ConflictClause();
        self::assertSame(['id', 'Email'], $clause->indexColumns('id, "Email"'));
        self::assertNull($clause->indexColumns('lower(email)'));
    }

    public function testIndexTargetCarriesThePartialPredicate(): void
    {
        $sql = '(id) WHERE active DO UPDATE SET id = excluded.id';
        $tokens = \ZtdQuery\Sql\SqlTokenStream::tokenize($sql, \ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::create())->significantTokens();
        $target = (new \ZtdQuery\Platform\Postgres\Sql\Statement\ConflictClause())->indexTarget($sql, $tokens, 2, ['id']);
        self::assertNotNull($target);
        self::assertSame(['id'], $target->columns);
        self::assertSame('active', $target->predicate);
    }

    public function testHasOnConflictIgnoresCommentedClauses(): void
    {
        $parser = new \ZtdQuery\Platform\Postgres\Sql\Statement\ConflictClause();
        self::assertSame(true, $parser->hasOnConflict('INSERT INTO users VALUES (1) ON CONFLICT DO NOTHING'));
        self::assertSame(false, $parser->hasOnConflict('INSERT INTO users VALUES (1) /* ON CONFLICT DO NOTHING */'));
    }
}
