<?php

declare(strict_types=1);

namespace Tests\Unit;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Platform\Postgres\PgSqlPdoPlaceholderEscaper;

#[CoversClass(PgSqlPdoPlaceholderEscaper::class)]
final class PgSqlPdoPlaceholderEscaperTest extends TestCase
{
    public function testEscapesPostgreSqlJsonExistenceOperators(): void
    {
        $sql = "SELECT meta ? 'reviewed', meta ?| array['author', 'reviewed'], meta ?& array['author', 'reviewed'] FROM docs";

        self::assertSame(
            "SELECT meta ?? 'reviewed', meta ??| array['author', 'reviewed'], meta ??& array['author', 'reviewed'] FROM docs",
            (new PgSqlPdoPlaceholderEscaper())->escape($sql),
        );
    }

    public function testPreservesPositionalPlaceholdersInOperandPositions(): void
    {
        $sql = 'SELECT ? FROM docs WHERE id = ? AND score BETWEEN ? AND ? ORDER BY ? LIMIT ? OFFSET ?';

        self::assertSame($sql, (new PgSqlPdoPlaceholderEscaper())->escape($sql));
    }

    public function testDistinguishesJsonOperatorFromItsPlaceholderOperand(): void
    {
        $sql = 'SELECT * FROM docs WHERE meta ? ? AND details ? :key AND id = ?';

        self::assertSame(
            'SELECT * FROM docs WHERE meta ?? ? AND details ?? :key AND id = ?',
            (new PgSqlPdoPlaceholderEscaper())->escape($sql),
        );
    }

    public function testLeavesQuotedAndCommentedQuestionMarksUntouched(): void
    {
        $sql = "SELECT '?' AS literal, \"?\" FROM docs -- ?\nWHERE body = \$tag\$?\$tag\$ AND meta ? 'key' /* ? */";

        self::assertSame(
            "SELECT '?' AS literal, \"?\" FROM docs -- ?\nWHERE body = \$tag\$?\$tag\$ AND meta ?? 'key' /* ? */",
            (new PgSqlPdoPlaceholderEscaper())->escape($sql),
        );
    }

    public function testPreservesAlreadyEscapedOperatorAndNativeParameters(): void
    {
        $sql = "SELECT * FROM docs WHERE meta ?? ? AND id = \$1 AND note = E'question\\'? mark'";

        self::assertSame($sql, (new PgSqlPdoPlaceholderEscaper())->escape($sql));
    }

    #[DataProvider('providerQuestionMarkBoundaries')]
    public function testHandlesQuestionMarkBoundaries(string $sql, string $expected): void
    {
        self::assertSame($expected, (new PgSqlPdoPlaceholderEscaper())->escape($sql));
    }

    #[DataProvider('providerCommentForms')]
    public function testPreservesQuestionMarksInCommentForms(string $sql, string $expected): void
    {
        self::assertSame($expected, (new PgSqlPdoPlaceholderEscaper())->escape($sql));
    }

    #[DataProvider('providerQuotedForms')]
    public function testPreservesQuestionMarksInQuotedForms(string $sql, string $expected): void
    {
        self::assertSame($expected, (new PgSqlPdoPlaceholderEscaper())->escape($sql));
    }

    #[DataProvider('providerDollarQuotedStringsAndNativeParameters')]
    public function testPreservesDollarQuotedStringsAndNativeParameters(string $sql, string $expected): void
    {
        self::assertSame($expected, (new PgSqlPdoPlaceholderEscaper())->escape($sql));
    }

    #[DataProvider('providerOperands')]
    public function testTracksNamedParametersIdentifiersAndNumbersAsOperands(string $sql, string $expected): void
    {
        self::assertSame($expected, (new PgSqlPdoPlaceholderEscaper())->escape($sql));
    }

    #[DataProvider('providerKeywords')]
    public function testKeywordsRequireAFollowingOperand(string $sql): void
    {
        self::assertSame($sql, (new PgSqlPdoPlaceholderEscaper())->escape($sql));
    }

    #[DataProvider('providerPunctuation')]
    public function testPunctuationDeterminesWhetherAnOperandFollows(string $sql, string $expected): void
    {
        self::assertSame($expected, (new PgSqlPdoPlaceholderEscaper())->escape($sql));
    }

    /**
     * @return \Generator<string, array{string, string}>
     */
    public static function providerQuestionMarkBoundaries(): \Generator
    {
        yield 'empty query' => ['', ''];
        yield 'placeholder' => ['?', '?'];
        yield 'escaped question mark' => ['??', '??'];
        yield 'any operator' => ['?|', '??|'];
        yield 'all operator' => ['?&', '??&'];
        yield 'operator after operand' => ['meta ?', 'meta ??'];
        yield 'operator with placeholder operand' => ['meta ? ?', 'meta ?? ?'];
        yield 'alternating operators and placeholders' => ['meta ? ? ?', 'meta ?? ? ??'];
        yield 'escaped operator with placeholder' => ['meta ?? ?', 'meta ?? ?'];
        yield 'any operator with placeholder' => ["meta ?| ? 'key'", "meta ??| ? 'key'"];
        yield 'all operator with placeholder' => ["meta ?& ? 'key'", "meta ??& ? 'key'"];
        yield 'opening parenthesis' => ['(?', '(?'];
        yield 'opening bracket' => ['[?', '[?'];
        yield 'colon' => ['meta : ?', 'meta : ??'];
        yield 'cast with placeholder' => ['meta::? ?', 'meta::? ??'];
    }

    /**
     * @return \Generator<string, array{string, string}>
     */
    public static function providerCommentForms(): \Generator
    {
        yield 'line comment' => ['-- ?', '-- ?'];
        yield 'line comment with carriage return' => ["-- ?\rSELECT ?", "-- ?\rSELECT ?"];
        yield 'line comment preserving state before newline' => ["meta--meta ?\n? 'key'", "meta--meta ?\n?? 'key'"];
        yield 'line comment preserving state before carriage return' => ["meta--meta ?\r? 'key'", "meta--meta ?\r?? 'key'"];
        yield 'line comment at end of input' => ['meta--meta ?', 'meta--meta ?'];
        yield 'separated minus signs' => ['meta- -meta ?', 'meta- -meta ??'];
        yield 'block comment' => ['/* ? */ SELECT ?', '/* ? */ SELECT ?'];
        yield 'nested block comment' => ['/* outer ? /* nested ? */ tail ? */ SELECT ?', '/* outer ? /* nested ? */ tail ? */ SELECT ?'];
        yield 'block comment preserving state' => ["meta/*meta ?*/? 'key'", "meta/*meta ?*/?? 'key'"];
        yield 'empty block comment' => ["meta/**/? 'key'", "meta/**/?? 'key'"];
        yield 'nested block comment preserving state' => ["meta/*outer /* inner meta ? */ outer meta ?*/? 'key'", "meta/*outer /* inner meta ? */ outer meta ?*/?? 'key'"];
        yield 'minimal block comment' => ['/**/', '/**/'];
        yield 'unterminated minimal block comment' => ['/*/', '/*/'];
        yield 'unterminated block comment ending in asterisk' => ['/**', '/**'];
        yield 'unterminated nested block comment' => ['/*/*/', '/*/*/'];
        yield 'closed nested block comment' => ['/*/**/*/', '/*/**/*/'];
        yield 'unterminated block comment' => ['SELECT 1 /* unterminated ?', 'SELECT 1 /* unterminated ?'];
        yield 'unterminated block comment preserving state' => ['meta/*unterminated meta ?', 'meta/*unterminated meta ?'];
        yield 'separated slash and asterisk' => ["meta/ *meta ?*/? 'key'", "meta/ *meta ??*/? 'key'"];
        yield 'comment between operator operands' => ["meta /* ? */ ? 'key'", "meta /* ? */ ?? 'key'"];
    }

    /**
     * @return \Generator<string, array{string, string}>
     */
    public static function providerQuotedForms(): \Generator
    {
        yield 'single quoted question mark' => ["'?' ? 'key'", "'?' ?? 'key'"];
        yield 'single quoted expression' => ["'meta ?' ? 'key'", "'meta ?' ?? 'key'"];
        yield 'doubled single quote' => ["'it''s ?' ? 'key'", "'it''s ?' ?? 'key'"];
        yield 'expression after doubled single quote' => ["'meta ''nested ?' ? 'key'", "'meta ''nested ?' ?? 'key'"];
        yield 'double quoted question mark' => ["\"?\" ? 'key'", "\"?\" ?? 'key'"];
        yield 'double quoted expression' => ["\"meta ?\" ? 'key'", "\"meta ?\" ?? 'key'"];
        yield 'doubled double quote' => ["\"a\"\"?\" ? 'key'", "\"a\"\"?\" ?? 'key'"];
        yield 'expression after doubled double quote' => ["\"meta \"\"nested ?\" ? 'key'", "\"meta \"\"nested ?\" ?? 'key'"];
        yield 'uppercase escape string' => ["E'meta \\' quoted ?' ? 'key'", "E'meta \\' quoted ?' ?? 'key'"];
        yield 'lowercase escape string' => ["e'meta \\' quoted ?' ? 'key'", "e'meta \\' quoted ?' ?? 'key'"];
        yield 'escape string after whitespace' => [" E'meta \\' quoted ?' ? 'key'", " E'meta \\' quoted ?' ?? 'key'"];
        yield 'escape string after operator' => ["!E'meta \\' quoted ?' ? 'key'", "!E'meta \\' quoted ?' ?? 'key'"];
        yield 'standard string backslash is not an escape' => ["'\\' meta ?' ? 'key'", "'\\' meta ??' ? 'key'"];
        yield 'quoted identifier backslash is not an escape' => ['"meta \\" rest ?" ? \'key\'', '"meta \\" rest ??" ? \'key\''];
        yield 'escape string ending in backslash' => ["E'meta \\", "E'meta \\"];
        yield 'escaped quote before trailing question mark' => ["E'abc \\'?", "E'abc \\'?"];
        yield 'escaped character before closing quote' => ["E'\\a'?", "E'\\a'??"];
        yield 'escape semantics survive doubled quote' => ["E'a''\\' meta ?' ? 'key'", "E'a''\\' meta ?' ?? 'key'"];
        yield 'identifier E suffix is not an escape prefix' => ["AE'meta \\' quoted ?' ? 'key'", "AE'meta \\' quoted ??' ? 'key'"];
        yield 'prefixed identifier E suffix is not an escape prefix' => ["!AE'meta \\' quoted ?' ? 'key'", "!AE'meta \\' quoted ??' ? 'key'"];
        yield 'operator delimits an escape prefix' => ["A!E'meta \\' quoted ?' ? 'key'", "A!E'meta \\' quoted ?' ?? 'key'"];
        yield 'number E suffix is not an escape prefix' => ["0E'meta \\' quoted ?' ? 'key'", "0E'meta \\' quoted ??' ? 'key'"];
        yield 'dollar E suffix is not an escape prefix' => ["\$E'meta \\' quoted ?' ? 'key'", "\$E'meta \\' quoted ??' ? 'key'"];
        yield 'unterminated doubled single quote' => ["'a''", "'a''"];
        yield 'unterminated doubled double quote' => ['"a""', '"a""'];
        yield 'unterminated single quote' => ["'unterminated ?", "'unterminated ?"];
        yield 'unterminated single quoted expression' => ["'unterminated meta ?", "'unterminated meta ?"];
        yield 'unterminated double quote' => ["\"unterminated ?", "\"unterminated ?"];
        yield 'unterminated double quoted expression' => ["\"unterminated meta ?", "\"unterminated meta ?"];
    }

    /**
     * @return \Generator<string, array{string, string}>
     */
    public static function providerDollarQuotedStringsAndNativeParameters(): \Generator
    {
        yield 'untagged dollar quote' => ["\$\$meta ?\$\$ ? 'key'", "\$\$meta ?\$\$ ?? 'key'"];
        yield 'unterminated dollar quote' => ["\$tag\$ ?", "\$tag\$ ?"];
        yield 'prefixed unterminated dollar quote' => ["prefix \$tag\$meta ?", "prefix \$tag\$meta ?"];
        yield 'tagged dollar quote' => ["\$tag\$meta ?\$tag\$ ? 'key'", "\$tag\$meta ?\$tag\$ ?? 'key'"];
        yield 'prefixed tagged dollar quote' => ["prefix \$A\$meta ?\$A\$ ? 'key'", "prefix \$A\$meta ?\$A\$ ?? 'key'"];
        yield 'standalone dollar' => ['$', '$'];
        yield 'unterminated dollar tag' => ['$A', '$A'];
        yield 'empty dollar quoted string' => ['$A$', '$A$'];
        yield 'empty dollar quote followed by dollar identifier' => ['$A$$A$B$?', '$A$$A$B$??'];
        yield 'one digit native parameter' => ["\$1 ? ?", "\$1 ?? ?"];
        yield 'two digit native parameter' => ["\$19 ? ?", "\$19 ?? ?"];
        yield 'zero native parameter boundary' => ["\$0? 'key'", "\$0?? 'key'"];
        yield 'nine native parameter boundary' => ["\$9? 'key'", "\$9?? 'key'"];
        yield 'ten native parameter boundary' => ["\$10? 'key'", "\$10?? 'key'"];
        yield 'ninety native parameter boundary' => ["\$90? 'key'", "\$90?? 'key'"];
        yield 'invalid numeric dollar tag' => ["\$9\$ ? 'key'", "\$9\$ ?? 'key'"];

        foreach (['tag', '_tag', 'A0', 'Z9', 'a0', 'z9'] as $tag) {
            $sql = sprintf('$%1$s$meta ?$%1$s$ ? \'key\'', $tag);
            $expected = sprintf('$%1$s$meta ?$%1$s$ ?? \'key\'', $tag);
            yield $tag => [$sql, $expected];
        }
    }

    /**
     * @return \Generator<string, array{string, string}>
     */
    public static function providerOperands(): \Generator
    {
        foreach ([':name', ':a_b9$', 'A', 'Z', 'a', 'z', '_value', 'value0', 'value9', 'value$', '0', '9', '123.45'] as $operand) {
            yield $operand . ' separated' => [$operand . " ? 'key'", $operand . " ?? 'key'"];
        }

        foreach ([':A0', ':Z9', ':a0', ':z9', ':_value', ':value$', 'A', 'Z', 'a', 'z', '_value', 'value0', 'value9', 'value$', '0', '9', '10', '90', '1.', '1.0'] as $operand) {
            yield $operand . ' adjacent' => [$operand . "? 'key'", $operand . "?? 'key'"];
        }

        yield 'cast' => ["value::jsonb ? 'key'", "value::jsonb ?? 'key'"];
        yield 'keyword named parameter' => [":SELECT ? 'key'", ":SELECT ?? 'key'"];
        yield 'adjacent keyword named parameter' => [":FROM? 'key'", ":FROM?? 'key'"];
        yield 'standalone colon' => [':', ':'];
    }

    /**
     * @return \Generator<string, array{string}>
     */
    public static function providerKeywords(): \Generator
    {
        $keywords = [
            'ALL',
            'AND',
            'ANY',
            'AS',
            'BETWEEN',
            'BY',
            'CASE',
            'CONFLICT',
            'DELETE',
            'DISTINCT',
            'DO',
            'ELSE',
            'FIRST',
            'FROM',
            'HAVING',
            'ILIKE',
            'IN',
            'INSERT',
            'INTO',
            'IS',
            'JOIN',
            'LAST',
            'LIKE',
            'LIMIT',
            'NOT',
            'OFFSET',
            'ON',
            'OR',
            'RETURNING',
            'SELECT',
            'SET',
            'SIMILAR',
            'SOME',
            'THEN',
            'UPDATE',
            'USING',
            'VALUE',
            'VALUES',
            'WHEN',
            'WHERE',
            'ZONE',
        ];

        foreach ($keywords as $keyword) {
            yield $keyword . ' uppercase' => [$keyword . ' ?'];
            yield $keyword . ' lowercase' => [strtolower($keyword) . ' ?'];
        }
    }

    /**
     * @return \Generator<string, array{string, string}>
     */
    public static function providerPunctuation(): \Generator
    {
        foreach (['(', '[', ',', ';', '.', '=', '<', '>', '!', '~', '+', '-', '*', '/', '%', '^', '|', '&', '#', '@'] as $prefix) {
            yield $prefix => ['meta' . $prefix . '?', 'meta' . $prefix . '?'];
        }

        yield 'closing parenthesis' => ["() ? 'key'", "() ?? 'key'"];
        yield 'closing bracket' => ["[] ? 'key'", "[] ?? 'key'"];
    }
}
