<?php

declare(strict_types=1);

namespace Tests\Unit\Connection\Copy;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Platform\Postgres\Connection\Copy\PgSqlCopySupport;
use ZtdQuery\Schema\TableDefinition;

#[CoversClass(PgSqlCopySupport::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::class)]
#[CoversClass(\ZtdQuery\Platform\Postgres\Connection\Copy\TargetColumns::class)]
#[CoversClass(\ZtdQuery\Platform\Postgres\Connection\Copy\TargetSql::class)]
#[CoversClass(\ZtdQuery\Platform\Postgres\Connection\Copy\TextFields::class)]
final class PgSqlCopySupportTest extends TestCase
{
    public function testTargetRelationRequotesEmbeddedIdentifierQuotes(): void
    {
        $support = new PgSqlCopySupport();
        $target = $support->target(
            '"Odd Schema"."a""b"',
            null,
            new TableDefinition(['id'], ['id' => 'INTEGER'], ['id'], ['id'], []),
        );

        self::assertSame(
            'SELECT "id" FROM "Odd Schema"."a""b"',
            $support->selectSql($target),
        );
    }





    public function testSelectSqlColumnsExcludeGeneratedColumnsAndParseExplicitFields(): void
    {
        $codec = new PgSqlCopySupport();
        $definition = new TableDefinition(
            ['id', 'display_name', 'computed'],
            ['id' => 'INTEGER', 'display_name' => 'TEXT', 'computed' => 'TEXT'],
            ['id'],
            ['id'],
            [],
            generatedExpressions: ['computed' => 'display_name'],
        );

        self::assertSame(['id', 'display_name'], $codec->target('items', null, $definition)->columns);
        $target = $codec->target('items', 'ID, "Display Name"', $definition);
        self::assertSame(['id', 'Display Name'], $target->columns);
        self::assertSame('SELECT "id", "Display Name" FROM "items"', $codec->selectSql($target));
    }











    public function testEncodeRowUsesPostgreSqlTextEscapesAndNativeScalarOutput(): void
    {
        $stream = fopen('php://memory', 'r+');
        self::assertIsResource($stream);
        self::assertSame(2, fwrite($stream, "\0\xff"));
        rewind($stream);

        $row = (new PgSqlCopySupport())->encodeRow(
            ["a\tb\nc\\d", null, true, false, $stream],
            '|',
            '\\N',
        );

        self::assertSame("a\\tb\\nc\\\\d|\\N|t|f|\\\\x00ff\n", $row);
    }

    public function testEncodeRowEscapesEveryPostgreSqlControlAndDelimiter(): void
    {
        self::assertSame(
            "\\b\\f\\n\\r\\t\\v\\\\\\|\n",
            (new PgSqlCopySupport())->encodeRow(["\x08\x0C\n\r\t\x0B\\|"], '|', '\\N'),
        );
        self::assertSame(
            "7|1.5\n",
            (new PgSqlCopySupport())->encodeRow([7, 1.5], '|', '\\N'),
        );
    }





    public function testDecodeRowHonorsNullBeforeEscapesAndSupportsByteEscapes(): void
    {
        $values = (new PgSqlCopySupport())->decodeRow(
            "\\N|\\\\N|a\\|b|line\\nfeed|\\101\\x42\r\n",
            '|',
            '\\N',
        );

        self::assertSame([null, '\\N', 'a|b', "line\nfeed", 'AB'], $values);
    }

    public function testDecodeRowSupportsEveryControlUnknownAndVariableLengthByteEscape(): void
    {
        $codec = new PgSqlCopySupport();

        self::assertSame(
            ["\x08\x0C\n\r\t\x0B\\|q"],
            $codec->decodeRow('\\b\\f\\n\\r\\t\\v\\\\\\|\\q', '|', '\\N'),
        );
        self::assertSame(
            ["\0", "\x07", "\n", 'A4', "\x04", 'O', 'Oa'],
            $codec->decodeRow('\\0|\\7|\\12|\\1014|\\x4|\\x4F|\\x4Fa', '|', '\\N'),
        );
        self::assertSame(['S', 'tail'], $codec->decodeRow('\\1232tail', '2', '\\N'));
        self::assertSame(
            ['prefixN', 'octal-A', 'hex-A'],
            $codec->decodeRow('prefix\\N|octal-\\101|hex-\\x41', '|', '\\N'),
        );
        self::assertSame(
            ["\xff", '?', "\0", "\xff"],
            $codec->decodeRow('\\377|\\77|\\x0|\\xff', '|', '\\N'),
        );
        self::assertSame(["\x01/"], $codec->decodeRow('\\1/', '|', '\\N'));
        self::assertSame([null], $codec->decodeRow('prefix\\101', '|', 'prefix\\101'));
        self::assertSame([null], $codec->decodeRow('prefix\\x41', '|', 'prefix\\x41'));
        self::assertSame(['value', null, 'tail'], $codec->decodeRow('value|NULL|tail', '|', 'NULL'));
    }

    #[DataProvider('providerLineEnding')]
    public function testDecodeRowAcceptsEverySupportedLineEnding(string $lineEnding): void
    {
        self::assertSame(['value'], (new PgSqlCopySupport())->decodeRow('value' . $lineEnding, '|', '\\N'));
    }



    public function testDecodeRowPreservesEmptyFields(): void
    {
        self::assertSame(['', ''], (new PgSqlCopySupport())->decodeRow('|', '|', '\\N'));
    }



    /**
     * @return iterable<string, array{string}>
     */
    public static function providerInvalidRelation(): iterable
    {
        yield 'empty' => [''];
        yield 'trailing qualifier' => ['users.'];
        yield 'operator' => ['users+other'];
        yield 'too many qualifiers' => ['catalog.public.users'];
        yield 'non-identifier' => ['*'];
    }

    /**
     * @return iterable<string, array{string}>
     */
    public static function providerInvalidFields(): iterable
    {
        yield 'empty' => [''];
        yield 'trailing comma' => ['id,'];
        yield 'expression' => ['id + value'];
        yield 'duplicate' => ['id, id'];
        yield 'non-identifier' => ['*'];
    }

    /**
     * @return iterable<string, array{string}>
     */
    public static function providerLineEnding(): iterable
    {
        yield 'none' => [''];
        yield 'line feed' => ["\n"];
        yield 'carriage return' => ["\r"];
        yield 'carriage return and line feed' => ["\r\n"];
    }

    /**
     * @return iterable<string, array{string}>
     */
    public static function providerInvalidRow(): iterable
    {
        yield 'unescaped line feed' => ["first\nsecond"];
        yield 'unescaped carriage return' => ["first\rsecond"];
        yield 'incomplete escape' => ['value\\'];
        yield 'end marker' => ['\\.'];
        yield 'octal overflow' => ['\\400'];
    }
    public function testTableNameResolvesTheFinalQualifiedComponent(): void
    {
        self::assertSame('Users', (new PgSqlCopySupport())->tableName('public."Users"'));
    }

    public function testInsertSqlNumbersEveryRowAndPreservesIdentityOverride(): void
    {
        $target = new \ZtdQuery\Platform\CopyTarget(['public', 'users'], ['id', 'name']);
        self::assertSame('INSERT INTO "public"."users" ("id", "name") OVERRIDING SYSTEM VALUE VALUES ($1, $2), ($3, $4)', (new PgSqlCopySupport())->insertSql($target, 2, true));
    }

    public function testIsCopyStatementReadsTheFirstSignificantKeyword(): void
    {
        $copy = new PgSqlCopySupport();
        self::assertTrue($copy->isCopyStatement('/* input */ COPY users FROM STDIN'));
        self::assertFalse($copy->isCopyStatement("SELECT 'COPY users'"));
    }
}
