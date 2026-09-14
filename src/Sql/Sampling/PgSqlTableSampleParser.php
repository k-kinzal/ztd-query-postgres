<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Sql\Sampling;

use ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile;
use ZtdQuery\Platform\Postgres\Sql\Relation\PgSqlSelectRelationParser;
use ZtdQuery\Sql\SqlTokenStream;

/**
 * Table sample parser for PostgreSQL queries.
 */
final class PgSqlTableSampleParser
{
    /**
     * @return list<PgSqlTableSample>
     */
    public function parse(string $sql): array
    {
        $stream = SqlTokenStream::tokenize($sql, PgSqlLexerProfile::create());
        $tokens = $stream->significantTokens();
        $samples = [];

        foreach ((new PgSqlSelectRelationParser())->references($sql) as $reference) {
            $referenceToken = (new SampleTokens())->tokenAtOffset($tokens, $reference['unqualifiedStart']);
            if ($referenceToken === null) {
                continue;
            }
            $sampleIndex = (new SampleTokens())->sampleIndexAfter($tokens, $referenceToken);
            if ($sampleIndex === null) {
                continue;
            }
            $samples[] = (new SampleClause())->parseSample($sql, $tokens, $reference, $sampleIndex, $referenceToken);
        }

        return $samples;
    }
}
