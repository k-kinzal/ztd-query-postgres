<?php

declare(strict_types=1);

namespace Tests\Unit;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Platform\Postgres\PgSqlPartitionParser;
use ZtdQuery\Schema\TablePartitionKey;
use ZtdQuery\Schema\TablePartitionStrategy;
use ZtdQuery\Sql\SqlToken;
use ZtdQuery\Sql\SqlTokenKind;
use ZtdQuery\Sql\SqlTokenStream;

#[CoversClass(PgSqlPartitionParser::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlLexerProfile::class)]
final class PgSqlPartitionParserTest extends TestCase
{
    public function testParsesRangePartitionKeyAndBounds(): void
    {
        $parser = new PgSqlPartitionParser();
        $key = $parser->parseKey('CREATE TABLE logs (log_date DATE) PARTITION BY RANGE (log_date)');

        self::assertNotNull($key);
        self::assertSame(TablePartitionStrategy::Range, $key->strategy);
        self::assertSame(['log_date'], $key->expressions);

        $relation = $parser->parseRelation(
            "CREATE TABLE logs_2024 PARTITION OF public.logs FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')",
            $key,
        );

        self::assertNotNull($relation);
        self::assertSame('logs', $relation->parentTable);
        self::assertSame(
            "(log_date) >= '2024-01-01' AND (log_date) < '2025-01-01'",
            $relation->predicate,
        );
    }

    public function testParsesListPartitionIncludingNull(): void
    {
        $parser = new PgSqlPartitionParser();
        $key = new TablePartitionKey(TablePartitionStrategy::List, ['region']);

        $relation = $parser->parseRelation(
            "CREATE TABLE accounts_local PARTITION OF accounts FOR VALUES IN ('east', 'west', NULL)",
            $key,
        );

        self::assertNotNull($relation);
        self::assertSame("((region) IN ('east', 'west') OR (region) IS NULL)", $relation->predicate);
    }

    public function testParsesDefaultPartition(): void
    {
        $parser = new PgSqlPartitionParser();
        $key = new TablePartitionKey(TablePartitionStrategy::List, ['region']);

        $relation = $parser->parseRelation('CREATE TABLE accounts_other PARTITION OF accounts DEFAULT', $key);

        self::assertNotNull($relation);
        self::assertNull($relation->predicate);
    }

    public function testParsesFiniteMultiColumnRangeAsRowComparison(): void
    {
        $parser = new PgSqlPartitionParser();
        $key = new TablePartitionKey(TablePartitionStrategy::Range, ['year', 'month']);

        $relation = $parser->parseRelation(
            'CREATE TABLE metrics_2024 PARTITION OF metrics FOR VALUES FROM (2024, 1) TO (2025, 1)',
            $key,
        );

        self::assertNotNull($relation);
        self::assertSame('ROW(year, month) >= ROW(2024, 1) AND ROW(year, month) < ROW(2025, 1)', $relation->predicate);
    }

    public function testParsesUnboundedRange(): void
    {
        $parser = new PgSqlPartitionParser();
        $key = new TablePartitionKey(TablePartitionStrategy::Range, ['id']);

        $relation = $parser->parseRelation(
            'CREATE TABLE smallest PARTITION OF values_table FOR VALUES FROM (MINVALUE) TO (10)',
            $key,
        );

        self::assertNotNull($relation);
        self::assertSame('(id) < 10', $relation->predicate);
    }

    public function testRejectsHashAndMixedUnboundedRangeInsteadOfGuessing(): void
    {
        $parser = new PgSqlPartitionParser();

        self::assertNull($parser->parseRelation(
            'CREATE TABLE values_hash PARTITION OF values_table FOR VALUES WITH (MODULUS 4, REMAINDER 0)',
            new TablePartitionKey(TablePartitionStrategy::Hash, ['id']),
        ));
        self::assertNull($parser->parseRelation(
            'CREATE TABLE values_range PARTITION OF values_table FOR VALUES FROM (1, MINVALUE) TO (2, MAXVALUE)',
            new TablePartitionKey(TablePartitionStrategy::Range, ['id', 'sequence']),
        ));
    }

    public function testRejectsMalformedPartitionClauses(): void
    {
        $parser = new PgSqlPartitionParser();

        self::assertNull($parser->parseKey('CREATE TABLE logs (id INTEGER)'));
        self::assertNull($parser->parseKey('CREATE TABLE RANGE (id)'));
        self::assertNull($parser->parseKey('CREATE TABLE logs (id INTEGER) PARTITION BY UNKNOWN (id)'));
        self::assertNull($parser->parseKey('CREATE TABLE logs (id INTEGER) PARTITION BY RANGE ()'));
        self::assertNull($parser->parseKey('CREATE TABLE logs (id INTEGER) PARTITION BY RANGE [id])'));
        self::assertNull($parser->parentTable('CREATE TABLE logs (id INTEGER)'));
        self::assertNull($parser->parentTable('CREATE TABLE child PARTITION OF [parent] DEFAULT'));
        self::assertNull($parser->parseRelation(
            'CREATE TABLE logs_2024 PARTITION OF logs FOR VALUES FROM () TO (10)',
            new TablePartitionKey(TablePartitionStrategy::Range, ['id']),
        ));
    }

    public function testParsesAllKeyStrategies(): void
    {
        $parser = new PgSqlPartitionParser();

        self::assertSame(
            TablePartitionStrategy::List,
            $parser->parseKey('CREATE TABLE accounts (region TEXT) PARTITION BY LIST (region)')?->strategy,
        );
        self::assertSame(
            TablePartitionStrategy::Hash,
            $parser->parseKey('CREATE TABLE values_table (id INTEGER) PARTITION BY HASH (id)')?->strategy,
        );
    }

    public function testRejectsMalformedBoundsIndependently(): void
    {
        $parser = new PgSqlPartitionParser();
        $range = new TablePartitionKey(TablePartitionStrategy::Range, ['id']);
        $list = new TablePartitionKey(TablePartitionStrategy::List, ['id']);

        self::assertNull($parser->parseRelation(
            'CREATE TABLE child PARTITION OF parent FOR VALUES IN (1)',
            $range,
        ));
        self::assertNull($parser->parseRelation(
            'CREATE TABLE child PARTITION OF parent FOR VALUES IN (1) TO (2)',
            $range,
        ));
        self::assertNull($parser->parseRelation(
            'CREATE TABLE child PARTITION OF parent FOR VALUES FROM (1) IN (2)',
            $range,
        ));
        self::assertNull($parser->parseRelation(
            'CREATE TABLE child PARTITION OF parent FOR VALUES FROM (1) TO ()',
            $range,
        ));
        self::assertNull($parser->parseRelation(
            'CREATE TABLE child PARTITION OF parent FOR VALUES FROM () TO (2)',
            $range,
        ));
        self::assertNull($parser->parseRelation(
            'CREATE TABLE child PARTITION OF parent FOR VALUES FROM (1, 2) TO (3, 4)',
            $range,
        ));
        self::assertNull($parser->parseRelation(
            'CREATE TABLE child PARTITION OF parent FOR VALUES FROM (MAXVALUE) TO (MAXVALUE)',
            $range,
        ));
        self::assertNull($parser->parseRelation(
            'CREATE TABLE child PARTITION OF parent FOR VALUES FROM (MINVALUE) TO (MINVALUE)',
            $range,
        ));
        self::assertNull($parser->parseRelation(
            'CREATE TABLE child PARTITION OF parent FOR VALUES FROM (1) TO (2)',
            $list,
        ));
        self::assertNull($parser->parseRelation(
            'CREATE TABLE child PARTITION OF parent FOR VALUES IN ()',
            $list,
        ));
    }

    public function testListPredicatesDistinguishValuesNullAndWhitespace(): void
    {
        $parser = new PgSqlPartitionParser();
        $key = new TablePartitionKey(TablePartitionStrategy::List, ['region']);

        self::assertSame(
            "(region) IN ('east', 'west')",
            $parser->parseRelation(
                "CREATE TABLE east_west PARTITION OF accounts FOR VALUES IN ('east', 'west')",
                $key,
            )?->predicate,
        );
        self::assertSame(
            '(region) IS NULL',
            $parser->parseRelation(
                'CREATE TABLE unset_region PARTITION OF accounts FOR VALUES IN (NULL)',
                $key,
            )?->predicate,
        );
        self::assertSame(
            "((region) IN ('east') OR (region) IS NULL)",
            $parser->parseRelation(
                "CREATE TABLE unset_region PARTITION OF accounts FOR VALUES IN (NULL, 'east')",
                $key,
            )?->predicate,
        );
        self::assertSame(
            '(id) < 10',
            $parser->parseRelation(
                'CREATE TABLE low_values PARTITION OF values_table FOR VALUES FROM ( minvalue ) TO (10)',
                new TablePartitionKey(TablePartitionStrategy::Range, ['id']),
            )?->predicate,
        );
    }

    public function testIgnoresNestedPartitionKeywordsWhenFindingTopLevelClauses(): void
    {
        $parser = new PgSqlPartitionParser();
        $key = $parser->parseKey(
            'CREATE TABLE logs (id INTEGER CHECK (PARTITION BY)) PARTITION BY RANGE (id)',
        );

        self::assertNotNull($key);
        self::assertSame(TablePartitionStrategy::Range, $key->strategy);
        self::assertSame(['id'], $key->expressions);
        self::assertSame(
            '(id) IN (1)',
            $parser->parseRelation(
                'CREATE TABLE child (id INTEGER DEFAULT 0) PARTITION OF parent FOR VALUES IN (1)',
                new TablePartitionKey(TablePartitionStrategy::List, ['id']),
            )?->predicate,
        );
    }

    public function testSkipsIncompleteKeywordPairBeforeThePartitionClause(): void
    {
        $key = (new PgSqlPartitionParser())->parseKey(
            'CREATE TABLE logs (id INTEGER) PARTITION WRONG RANGE (wrong) PARTITION BY RANGE (id)',
        );

        self::assertNotNull($key);
        self::assertSame(TablePartitionStrategy::Range, $key->strategy);
        self::assertSame(['id'], $key->expressions);
    }

    public function testNormalizesUnquotedParentNamesAndPreservesQuotedNames(): void
    {
        $parser = new PgSqlPartitionParser();

        self::assertSame(
            'logs',
            $parser->parentTable('CREATE TABLE child PARTITION OF Public.Logs DEFAULT'),
        );
        self::assertSame(
            'Logs',
            $parser->parentTable('CREATE TABLE child PARTITION OF public."Logs" DEFAULT'),
        );
    }

    public function testQualifiedIdentifierRejectsMismatchedTokenStream(): void
    {
        $method = new \ReflectionMethod(PgSqlPartitionParser::class, 'qualifiedIdentifierAt');
        $stream = SqlTokenStream::tokenize('users', \ZtdQuery\Platform\Postgres\PgSqlLexerProfile::create());
        $tokens = [new SqlToken(SqlTokenKind::String, 'users', 0, 0, 0)];

        self::assertNull($method->invoke(new PgSqlPartitionParser(), $stream, $tokens, 0));
    }
}
