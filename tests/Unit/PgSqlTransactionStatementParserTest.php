<?php

declare(strict_types=1);

namespace Tests\Unit;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Platform\Postgres\PgSqlLexerProfile;
use ZtdQuery\Platform\Postgres\PgSqlTransactionStatementParser;
use ZtdQuery\Shadow\ShadowStore;
use ZtdQuery\Shadow\ShadowTransactionManager;

#[CoversClass(PgSqlTransactionStatementParser::class)]
#[UsesClass(PgSqlLexerProfile::class)]
final class PgSqlTransactionStatementParserTest extends TestCase
{
    #[DataProvider('providerPostgresTransactionForms')]
    public function testParsesPostgresTransactionForms(string $sql): void
    {
        self::assertNotNull((new PgSqlTransactionStatementParser())->parse($sql));
    }

    /** @return array<string, array{string}> */
    public static function providerPostgresTransactionForms(): array
    {
        return [
            'begin' => ['BEGIN;'],
            'begin work' => ['BEGIN WORK'],
            'begin transaction' => ['BEGIN TRANSACTION'],
            'start' => ['START TRANSACTION'],
            'commit' => ['COMMIT'],
            'commit work' => ['COMMIT WORK'],
            'commit transaction' => ['COMMIT TRANSACTION'],
            'end' => ['END'],
            'end work' => ['END WORK'],
            'end transaction' => ['END TRANSACTION'],
            'rollback' => ['ROLLBACK'],
            'rollback work' => ['ROLLBACK WORK'],
            'rollback transaction' => ['ROLLBACK TRANSACTION'],
            'savepoint' => ['SAVEPOINT "a""b"'],
            'rollback to' => ['ROLLBACK TO point'],
            'rollback to savepoint' => ['ROLLBACK TO SAVEPOINT point'],
            'release' => ['RELEASE point'],
            'release savepoint' => ['RELEASE SAVEPOINT point'],
        ];
    }

    public function testRejectsNonPostgresAndMalformedForms(): void
    {
        $parser = new PgSqlTransactionStatementParser();

        self::assertNull($parser->parse(''));
        self::assertNull($parser->parse('BEGIN IMMEDIATE'));
        self::assertNull($parser->parse('SAVEPOINT `point`'));
        self::assertNull($parser->parse('SAVEPOINT point extra'));
        self::assertNull($parser->parse('SAVEPOINT "broken'));
    }

    public function testAppliesUnescapedPostgresSavepointName(): void
    {
        $store = new ShadowStore();
        $store->set('items', [['id' => 1]]);
        $transactions = new ShadowTransactionManager($store);
        $transactions->begin();
        $statement = (new PgSqlTransactionStatementParser())->parse('SAVEPOINT "a""b"');
        self::assertNotNull($statement);
        $statement->apply($transactions);
        $store->insert('items', [['id' => 2]]);

        $transactions->rollBackTo('a"b');

        self::assertSame([['id' => 1]], $store->get('items'));
    }
}
