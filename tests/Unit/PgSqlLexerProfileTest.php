<?php

declare(strict_types=1);

namespace Tests\Unit;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Platform\Postgres\PgSqlLexerProfile;
use ZtdQuery\Sql\SqlToken;
use ZtdQuery\Sql\SqlTokenKind;
use ZtdQuery\Sql\SqlTokenStream;

#[CoversClass(PgSqlLexerProfile::class)]
final class PgSqlLexerProfileTest extends TestCase
{
    public function testSelectsPostgreSqlLexicalCapabilities(): void
    {
        $profile = PgSqlLexerProfile::create();
        $tokens = SqlTokenStream::tokenize(
            'SELECT `plain`, "quoted", $$body;still-string$$, $1',
            $profile,
        )->significantTokens();

        self::assertSame(
            [
                SqlTokenKind::Word,
                SqlTokenKind::Symbol,
                SqlTokenKind::Word,
                SqlTokenKind::Symbol,
                SqlTokenKind::Symbol,
                SqlTokenKind::QuotedIdentifier,
                SqlTokenKind::Symbol,
                SqlTokenKind::String,
                SqlTokenKind::Symbol,
                SqlTokenKind::Parameter,
            ],
            array_map(static fn (SqlToken $token): SqlTokenKind => $token->kind, $tokens),
        );
        self::assertSame(
            ["'a\\'", 'tail'],
            array_map(
                static fn (SqlToken $token): string => $token->text,
                SqlTokenStream::tokenize("'a\\' tail", $profile)->significantTokens(),
            ),
        );
        self::assertSame(
            ['E', "'a\\'b'", 'tail'],
            array_map(
                static fn (SqlToken $token): string => $token->text,
                SqlTokenStream::tokenize("E'a\\'b' tail", $profile)->significantTokens(),
            ),
        );
        self::assertTrue($profile->startsLineComment('-- comment', 0));
        self::assertSame(['/*', '*/'], $profile->blockCommentAt('/* comment */', 0));
        self::assertTrue($profile->isBracketOpening('['));
        self::assertTrue($profile->isBracketClosing(']'));
        self::assertTrue($profile->isNestingOpening('('));
        self::assertTrue($profile->isNestingClosing(')'));
        self::assertTrue($profile->supportsNestedBlockComments());
        self::assertSame(2, $profile->positionalParameterLengthAt('$1', 0));
        self::assertSame(1, $profile->positionalParameterLengthAt('?1', 0));
    }

    public function testRecognizesRadixNumbersAndDoesNotMisclassifyCastOperator(): void
    {
        $tokens = SqlTokenStream::tokenize(
            '0xCA_FE 0o_755 0b10_01 1_000.25_5 value::text :bound',
            PgSqlLexerProfile::create(),
        )->significantTokens();

        self::assertSame(
            ['0xCA_FE', '0o_755', '0b10_01', '1_000.25_5'],
            array_map(static fn (SqlToken $token): string => $token->text, array_slice($tokens, 0, 4)),
        );
        self::assertSame(
            [SqlTokenKind::Word, SqlTokenKind::Symbol, SqlTokenKind::Symbol, SqlTokenKind::Word, SqlTokenKind::Parameter],
            array_map(static fn (SqlToken $token): SqlTokenKind => $token->kind, array_slice($tokens, 4)),
        );
        self::assertSame(':bound', $tokens[8]->text);
    }
}
