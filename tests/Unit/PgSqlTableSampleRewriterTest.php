<?php

declare(strict_types=1);

namespace Tests\Unit;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Exception\UnsupportedSqlException;
use ZtdQuery\Platform\Postgres\PgSqlIdentifierQuoter;
use ZtdQuery\Platform\Postgres\PgSqlTableSample;
use ZtdQuery\Platform\Postgres\PgSqlTableSampleParser;
use ZtdQuery\Platform\Postgres\PgSqlTableSampleRewriter;

#[CoversClass(PgSqlTableSampleRewriter::class)]
#[UsesClass(PgSqlTableSampleParser::class)]
#[UsesClass(PgSqlTableSample::class)]
#[UsesClass(PgSqlIdentifierQuoter::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlSelectRelationParser::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\PgSqlLexerProfile::class)]
final class PgSqlTableSampleRewriterTest extends TestCase
{
    public function testRewritesBernoulliSampleAsDerivedShadowRelation(): void
    {
        $result = (new PgSqlTableSampleRewriter())->rewrite(
            'SELECT sampled.id FROM data AS sampled TABLESAMPLE BERNOULLI ($1)',
            ['data' => ['columns' => ['id', 'value']]],
        );

        self::assertStringNotContainsString('TABLESAMPLE', $result);
        self::assertStringContainsString('ROW_NUMBER() OVER () AS "__ztd_sample_ordinal"', $result);
        self::assertStringContainsString('FROM data', $result);
        self::assertStringContainsString('CAST(($1) AS DOUBLE PRECISION)', $result);
        self::assertStringContainsString('random() AS "__ztd_sample_seed"', $result);
        self::assertStringEndsWith(' AS sampled', $result);
        self::assertSame(
            '924604101f2f5928abaa874fa6d54f176e4e6d6afa2a81334267eea904a147eb',
            hash('sha256', $result),
        );
    }

    public function testRepeatableUsesProvidedSeedAndSystemUsesOneVirtualBlock(): void
    {
        $result = (new PgSqlTableSampleRewriter())->rewrite(
            'SELECT * FROM data TABLESAMPLE SYSTEM (50) REPEATABLE (7.5)',
            ['data' => ['columns' => ['id']]],
        );

        self::assertStringContainsString('CAST((7.5) AS DOUBLE PRECISION)', $result);
        self::assertStringContainsString('MD5(CAST(0 AS TEXT)', $result);
        self::assertStringEndsWith(' AS "data"', $result);
        self::assertSame(
            '0b97b0d5fe25795951b26924624bea41df8218e2f895764d11009aee7da164d6',
            hash('sha256', $result),
        );
    }

    public function testMultipleSamplesAreReplacedWithoutOffsetCorruption(): void
    {
        $result = (new PgSqlTableSampleRewriter())->rewrite(
            'SELECT * FROM data TABLESAMPLE BERNOULLI (100), logs TABLESAMPLE SYSTEM (0)',
            [
                'data' => ['columns' => ['id']],
                'logs' => ['columns' => ['id', 'message']],
            ],
        );

        self::assertStringNotContainsString('TABLESAMPLE', $result);
        self::assertSame(2, substr_count($result, 'ROW_NUMBER() OVER ()'));
        self::assertStringContainsString('FROM data', $result);
        self::assertStringContainsString('FROM logs', $result);
        self::assertSame(
            'd0e77ae3bc03c3a002cd20bbd7de31a6cce7d73b2ae8cc1e3764f111b931c4e7',
            hash('sha256', $result),
        );
    }

    public function testOrdinarySelectIsUnchanged(): void
    {
        $sql = 'SELECT * FROM data';

        self::assertSame($sql, (new PgSqlTableSampleRewriter())->rewrite($sql, []));
    }

    public function testRejectsSampleWhenColumnsAreUnknown(): void
    {
        $this->expectException(UnsupportedSqlException::class);
        $this->expectExceptionMessage('Cannot determine columns');

        (new PgSqlTableSampleRewriter())->rewrite(
            'SELECT * FROM data TABLESAMPLE BERNOULLI (50)',
            ['data' => ['columns' => []]],
        );
    }

    public function testIgnoresNonStringColumnMetadataAndReindexesNames(): void
    {
        $result = (new PgSqlTableSampleRewriter())->rewrite(
            'SELECT * FROM data TABLESAMPLE BERNOULLI (50)',
            ['data' => ['columns' => ['id', 123, 'value']]],
        );

        self::assertStringContainsString('SELECT "id", "value"', $result);
        self::assertStringNotContainsString('123', $result);
    }
}
