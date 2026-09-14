<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Sql\Sampling;

use ZtdQuery\Exception\UnsupportedSqlException;
use ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile;
use ZtdQuery\Sql\SqlToken;
use ZtdQuery\Sql\SqlTokenStream;

/**
 * Sample clause operations for PostgreSQL sampling.
 *
 * @visibility root
 */
final class SampleClause
{
    /**
     * @param list<SqlToken> $tokens
     * @param array{name: string, start: int, unqualifiedStart: int, end: int} $reference
     * @throws UnsupportedSqlException
     */
    public function parseSample(
        string $sql,
        array $tokens,
        array $reference,
        int $sampleIndex,
        SqlToken $referenceToken,
    ): PgSqlTableSample {
        $methodToken = $tokens[$sampleIndex + 1] ?? null;
        $method = $methodToken instanceof SqlToken
            ? PgSqlTableSampleMethod::tryFrom(strtoupper($methodToken->text))
            : null;
        if ($method === null) {
            throw new UnsupportedSqlException($sql, 'TABLESAMPLE method not supported for shadow relations');
        }

        $openIndex = $sampleIndex + 2;
        $open = $tokens[$openIndex] ?? null;
        if (!$open instanceof SqlToken || !(new SampleTokens())->isOpeningParenthesis($open, $referenceToken)) {
            throw new UnsupportedSqlException($sql, 'Malformed TABLESAMPLE opening parenthesis');
        }
        $closeIndex = (new SampleTokens())->closingParenthesisIndex($tokens, $openIndex);
        if ($closeIndex === null) {
            throw new UnsupportedSqlException($sql, 'Malformed TABLESAMPLE closing parenthesis');
        }
        $close = $tokens[$closeIndex];
        $percentageSql = trim(substr($sql, $open->endOffset(), $close->offset - $open->endOffset()));
        if ($percentageSql === '' || count(SqlTokenStream::tokenize($percentageSql, PgSqlLexerProfile::create())->splitTopLevel()) !== 1) {
            throw new UnsupportedSqlException($sql, 'TABLESAMPLE requires one percentage expression');
        }

        $repeatable = $this->repeatable($sql, $tokens, $referenceToken, $closeIndex);

        $sampleToken = $tokens[$sampleIndex];
        $aliasSql = $this->aliasSql($sql, $tokens, $reference, $referenceToken, $sampleToken);

        return new PgSqlTableSample(
            $reference['name'],
            substr($sql, $reference['start'], $reference['end'] - $reference['start']),
            $aliasSql,
            $method,
            $percentageSql,
            $repeatable['seed'],
            $reference['start'],
            $repeatable['end'],
        );
    }
    /**
     * Reads the optional REPEATABLE seed following the percentage expression.
     * @param list<SqlToken> $tokens
     * @return array{seed: string|null, end: int}
     * @throws UnsupportedSqlException
     */
    public function repeatable(string $sql, array $tokens, SqlToken $referenceToken, int $closeIndex): array
    {
        $seedSql = null;
        $endOffset = $tokens[$closeIndex]->endOffset();
        $repeatable = $tokens[$closeIndex + 1] ?? null;
        if ($repeatable?->isKeyword('REPEATABLE') === true
            && (new SampleTokens())->sameLevel($repeatable, $referenceToken)
        ) {
            $seedOpenIndex = $closeIndex + 2;
            $seedOpen = $tokens[$seedOpenIndex] ?? null;
            if (!$seedOpen instanceof SqlToken || !(new SampleTokens())->isOpeningParenthesis($seedOpen, $referenceToken)) {
                throw new UnsupportedSqlException($sql, 'Malformed TABLESAMPLE REPEATABLE opening parenthesis');
            }
            $seedCloseIndex = (new SampleTokens())->closingParenthesisIndex($tokens, $seedOpenIndex);
            if ($seedCloseIndex === null) {
                throw new UnsupportedSqlException($sql, 'Malformed TABLESAMPLE REPEATABLE closing parenthesis');
            }
            $seedClose = $tokens[$seedCloseIndex];
            $seedSql = trim(substr($sql, $seedOpen->endOffset(), $seedClose->offset - $seedOpen->endOffset()));
            if ($seedSql === '' || count(SqlTokenStream::tokenize($seedSql, PgSqlLexerProfile::create())->splitTopLevel()) !== 1) {
                throw new UnsupportedSqlException($sql, 'TABLESAMPLE REPEATABLE requires one seed expression');
            }
            $endOffset = $seedClose->endOffset();
        }

        return ['seed' => $seedSql, 'end' => $endOffset];
    }

    /**
     * Preserves the alias between a relation or inheritance marker and TABLESAMPLE.
     * @param list<SqlToken> $tokens
     * @param array{name: string, start: int, unqualifiedStart: int, end: int} $reference
     */
    public function aliasSql(string $sql, array $tokens, array $reference, SqlToken $referenceToken, SqlToken $sampleToken): string
    {
        $aliasStart = $reference['end'];
        $inheritanceMarker = (new SampleTokens())->tokenAfter($tokens, $referenceToken);
        if ($inheritanceMarker->text === '*') {
            $aliasStart = $inheritanceMarker->endOffset();
        }
        $aliasSql = trim(substr($sql, $aliasStart, $sampleToken->offset - $aliasStart));

        return $aliasSql;
    }
}
