<?php

declare(strict_types=1);

namespace Tests\Unit;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Platform\Postgres\PostgreSqlLexicalMasker;

#[CoversClass(PostgreSqlLexicalMasker::class)]
final class PostgreSqlLexicalMaskerTest extends TestCase
{
    #[DataProvider('providerComments')]
    public function testMasksCommentsAsSingleLexicalSeparators(string $sql, string $expected): void
    {
        self::assertSame($expected, PostgreSqlLexicalMasker::maskComments($sql));
    }

    #[DataProvider('providerQuotedForms')]
    public function testPreservesCommentMarkersInsideQuotedForms(string $quoted, bool $terminated): void
    {
        $sql = 'SELECT ' . $quoted . '/* outside */1';
        $expected = $terminated ? 'SELECT ' . $quoted . ' 1' : $sql;

        self::assertSame($expected, PostgreSqlLexicalMasker::maskComments($sql));
    }

    public function testPreservesEscapeStringAtStatementStart(): void
    {
        $sql = "E'escaped \\' /* protected */'";

        self::assertSame($sql, PostgreSqlLexicalMasker::maskComments($sql));
    }

    #[DataProvider('providerStringLiterals')]
    public function testMasksStringLiteralsWithoutChangingOffsets(string $sql, string $expected): void
    {
        $masked = PostgreSqlLexicalMasker::maskStringLiterals($sql);
        self::assertSame($expected, $masked);
        self::assertSame(strlen($sql), strlen($masked));
    }

    /**
     * @return \Generator<string, array{string, string}>
     */
    public static function providerComments(): \Generator
    {
        yield 'empty query' => ['', ''];
        yield 'line comment' => ['SELECT-- comment', 'SELECT '];
        yield 'line comment with newline' => ["SELECT-- comment\n1", "SELECT \n1"];
        yield 'line comment with carriage return' => ["SELECT-- comment\r1", "SELECT \r1"];
        yield 'empty line comment' => ["--\nSELECT", " \nSELECT"];
        yield 'separated minus signs' => ['SELECT- -1', 'SELECT- -1'];
        yield 'block comment' => ['SELECT/* comment */1', 'SELECT 1'];
        yield 'minimal block comment' => ['SELECT/**/1', 'SELECT 1'];
        yield 'nested block comment' => ['SELECT/* outer /* inner */ outer */1', 'SELECT 1'];
        yield 'adjacent block comments' => ['SELECT/**//**/1', 'SELECT  1'];
        yield 'unterminated block comment' => ['SELECT/* comment', 'SELECT '];
        yield 'unterminated nested block comment' => ['SELECT/* outer /* inner */', 'SELECT '];
        yield 'separated slash and asterisk' => ['SELECT/ *1', 'SELECT/ *1'];
        yield 'comment-like closing marker' => ['SELECT*/1', 'SELECT*/1'];
        yield 'comment closing boundary' => ['SELECT/**/*1', 'SELECT *1'];
        yield 'comment after identifier E suffix' => ["AE'closed'/* comment */", "AE'closed' "];
        yield 'comment after uppercase identifier E escape lookalike' => ["ZE'escaped \\'/* comment */1", "ZE'escaped \\' 1"];
        yield 'comment after lowercase identifier E escape lookalike' => ["zE'escaped \\'/* comment */1", "zE'escaped \\' 1"];
        yield 'comment after underscore E escape lookalike' => ["_E'escaped \\'/* comment */1", "_E'escaped \\' 1"];
        yield 'comment after number E suffix' => ["0E'closed'/* comment */", "0E'closed' "];
        yield 'comment after dollar E suffix' => ["\$E'closed'/* comment */", "\$E'closed' "];
        yield 'comment after native parameter' => ['$1/* comment */', '$1 '];
        yield 'comment between invalid numeric tags' => ['$9$/* comment */$9$', '$9$ $9$'];
        yield 'comment after invalid hyphenated tag' => ['$a-b$/* comment */1', '$a-b$ 1'];
        yield 'comment after incomplete tag' => ['$tag/* comment */1', '$tag 1'];
        yield 'incomplete tag at end' => ['$tag', '$tag'];
        yield 'standalone dollar at end' => ['$', '$'];
        yield 'normal string does not use backslash escapes' => ["'escaped \\'/* comment */1", "'escaped \\' 1"];
        yield 'qualified identifier E escape lookalike' => ["SELECT AE'escaped \\'/* comment */1", "SELECT AE'escaped \\' 1"];
        yield 'underscored identifier E escape lookalike' => ["SELECT _AE'escaped \\'/* comment */1", "SELECT _AE'escaped \\' 1"];
        yield 'identifier-attached dollar tag is not quoted' => ['name$tag$/* comment */$tag$', 'name$tag$ $tag$'];
        yield 'digit-attached dollar tag is not quoted' => ['0$tag$/* comment */$tag$', '0$tag$ $tag$'];
        yield 'underscore-attached dollar tag is not quoted' => ['_$tag$/* comment */$tag$', '_$tag$ $tag$'];
    }

    /**
     * @return \Generator<string, array{string, bool}>
     */
    public static function providerQuotedForms(): \Generator
    {
        yield 'single quoted line marker' => ["'-- comment'", true];
        yield 'single quoted block marker' => ["'/* comment */'", true];
        yield 'empty single quote' => ["''", true];
        yield 'doubled single quote' => ["'value''/* comment */'", true];
        yield 'triple single quote closing run' => ["'value'''", true];
        yield 'unterminated single quote' => ["'/* comment", false];
        yield 'double quoted line marker' => ['"-- comment"', true];
        yield 'double quoted block marker' => ['"/* comment */"', true];
        yield 'empty double quote' => ['""', true];
        yield 'doubled double quote' => ['"value""/* comment */"', true];
        yield 'unterminated double quote' => ['"/* comment', false];
        yield 'uppercase escape string' => ["E'escaped \\' /* comment */'", true];
        yield 'lowercase escape string' => ["e'escaped \\' -- comment'", true];
        yield 'escape string closes immediately after escaped quote' => ["E'\\''", true];
        yield 'escape string ending in backslash' => ["E'escaped \\", false];
        yield 'untagged dollar quote' => ['$$/* comment */$$', true];
        yield 'uppercase boundary dollar tag' => ['$A$-- comment$A$', true];
        yield 'uppercase ending boundary dollar tag' => ['$Z$-- comment$Z$', true];
        yield 'lowercase boundary dollar tag' => ['$a$-- comment$a$', true];
        yield 'lowercase ending boundary dollar tag' => ['$z$-- comment$z$', true];
        yield 'tagged dollar quote' => ['$tag$-- comment$tag$', true];
        yield 'delimiter prefix inside dollar quote' => ['$tag$value $tag/* protected */$tag$', true];
        yield 'identifier dollar quote' => ['$_tag9$/* comment */$_tag9$', true];
        yield 'unterminated dollar quote' => ['$tag$/* comment */', false];
        yield 'standalone dollar' => ['$', true];
    }

    /**
     * @return \Generator<string, array{string, string}>
     */
    public static function providerStringLiterals(): \Generator
    {
        yield 'empty query' => ['', ''];
        yield 'empty string' => ["''", '  '];
        yield 'single quoted string' => ["SELECT 'WHERE' FROM name", 'SELECT ' . str_repeat(' ', 7) . ' FROM name'];
        yield 'doubled single quote' => ["'value''WHERE' FROM name", str_repeat(' ', 14) . ' FROM name'];
        yield 'unterminated string' => ["'WHERE", str_repeat(' ', 6)];
        yield 'uppercase escape string' => ["E'escaped \\' WHERE' FROM name", 'E' . str_repeat(' ', 18) . ' FROM name'];
        yield 'lowercase escape string' => ["e'escaped \\' WHERE' FROM name", 'e' . str_repeat(' ', 18) . ' FROM name'];
        yield 'standard backslash does not escape quote' => ["'closed \\' WHERE name", str_repeat(' ', 10) . ' WHERE name'];
        yield 'untagged dollar quote' => ['$$WHERE$$ FROM name', str_repeat(' ', 9) . ' FROM name'];
        yield 'tagged dollar quote' => ['$tag$WHERE$tag$ FROM name', str_repeat(' ', 15) . ' FROM name'];
        yield 'prefixed untagged dollar quote' => ['SELECT $$WHERE$$ FROM name', 'SELECT ' . str_repeat(' ', 9) . ' FROM name'];
        yield 'prefixed tagged dollar quote' => ['SELECT $tag$WHERE$tag$ FROM name', 'SELECT ' . str_repeat(' ', 15) . ' FROM name'];
        yield 'unterminated dollar quote' => ['$tag$WHERE', str_repeat(' ', 10)];
        yield 'double quoted identifier' => ['"WHERE" FROM name', '"WHERE" FROM name'];
        yield 'native parameter' => ['$1 FROM name', '$1 FROM name'];
        yield 'invalid numeric tag' => ['$9$WHERE$9$ FROM name', '$9$WHERE$9$ FROM name'];
        yield 'identifier-attached tag' => ['name$tag$WHERE$tag$ FROM name', 'name$tag$WHERE$tag$ FROM name'];
        yield 'digit-attached tag' => ['0$tag$WHERE$tag$ FROM name', '0$tag$WHERE$tag$ FROM name'];
        yield 'underscore-attached tag' => ['_$tag$WHERE$tag$ FROM name', '_$tag$WHERE$tag$ FROM name'];
        yield 'newlines preserve length' => ["'line1\nline2' FROM name", str_repeat(' ', 13) . ' FROM name'];
    }
}
