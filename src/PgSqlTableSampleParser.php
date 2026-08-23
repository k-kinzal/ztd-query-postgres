<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres;

use ZtdQuery\Exception\UnsupportedSqlException;
use ZtdQuery\Sql\SqlToken;
use ZtdQuery\Sql\SqlTokenKind;
use ZtdQuery\Sql\SqlTokenStream;

final class PgSqlTableSampleParser
{
    /** @return list<PgSqlTableSample> */
    public function parse(string $sql): array
    {
        $stream = SqlTokenStream::tokenize($sql, PgSqlLexerProfile::create());
        $tokens = $stream->significantTokens();
        $samples = [];

        foreach ((new PgSqlSelectRelationParser())->references($sql) as $reference) {
            $referenceToken = $this->tokenAtOffset($tokens, $reference['unqualifiedStart']);
            if ($referenceToken === null) {
                continue;
            }
            $sampleIndex = $this->sampleIndexAfter($tokens, $referenceToken);
            if ($sampleIndex === null) {
                continue;
            }
            $samples[] = $this->parseSample($sql, $tokens, $reference, $sampleIndex, $referenceToken);
        }

        return $samples;
    }

    /**
     * @param list<SqlToken> $tokens
     * @param array{name: string, start: int, unqualifiedStart: int, end: int} $reference
     */
    private function parseSample(
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
        if (!$open instanceof SqlToken || !$this->isOpeningParenthesis($open, $referenceToken)) {
            throw new UnsupportedSqlException($sql, 'Malformed TABLESAMPLE opening parenthesis');
        }
        $closeIndex = $this->closingParenthesisIndex($tokens, $openIndex);
        if ($closeIndex === null) {
            throw new UnsupportedSqlException($sql, 'Malformed TABLESAMPLE closing parenthesis');
        }
        $close = $tokens[$closeIndex];
        $percentageSql = trim(substr($sql, $open->endOffset(), $close->offset - $open->endOffset()));
        if ($percentageSql === '' || count(SqlTokenStream::tokenize($percentageSql, PgSqlLexerProfile::create())->splitTopLevel()) !== 1) {
            throw new UnsupportedSqlException($sql, 'TABLESAMPLE requires one percentage expression');
        }

        $seedSql = null;
        $endOffset = $close->endOffset();
        $repeatable = $tokens[$closeIndex + 1] ?? null;
        if ($repeatable?->isKeyword('REPEATABLE') === true
            && $this->sameLevel($repeatable, $referenceToken)
        ) {
            $seedOpenIndex = $closeIndex + 2;
            $seedOpen = $tokens[$seedOpenIndex] ?? null;
            if (!$seedOpen instanceof SqlToken || !$this->isOpeningParenthesis($seedOpen, $referenceToken)) {
                throw new UnsupportedSqlException($sql, 'Malformed TABLESAMPLE REPEATABLE opening parenthesis');
            }
            $seedCloseIndex = $this->closingParenthesisIndex($tokens, $seedOpenIndex);
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

        $sampleToken = $tokens[$sampleIndex];
        $aliasStart = $reference['end'];
        $inheritanceMarker = $this->tokenAfter($tokens, $referenceToken);
        if ($inheritanceMarker->text === '*') {
            $aliasStart = $inheritanceMarker->endOffset();
        }
        $aliasSql = trim(substr($sql, $aliasStart, $sampleToken->offset - $aliasStart));

        return new PgSqlTableSample(
            $reference['name'],
            substr($sql, $reference['start'], $reference['end'] - $reference['start']),
            $aliasSql,
            $method,
            $percentageSql,
            $seedSql,
            $reference['start'],
            $endOffset,
        );
    }

    /** @param list<SqlToken> $tokens */
    private function tokenAtOffset(array $tokens, int $offset): ?SqlToken
    {
        foreach ($tokens as $token) {
            if ($token->offset === $offset) {
                return $token;
            }
        }

        return null;
    }

    /** @param list<SqlToken> $tokens */
    private function sampleIndexAfter(array $tokens, SqlToken $referenceToken): ?int
    {
        $afterReference = false;
        foreach ($tokens as $index => $token) {
            if ($token === $referenceToken) {
                $afterReference = true;
                continue;
            }
            if (!$afterReference || !$this->sameLevel($token, $referenceToken)) {
                continue;
            }
            if ($token->isKeyword('TABLESAMPLE')) {
                return $index;
            }
            if ($token->text === ',' || $this->isRelationBoundary($token)) {
                return null;
            }
        }

        return null;
    }

    private function isRelationBoundary(SqlToken $token): bool
    {
        foreach (['JOIN', 'ON', 'USING', 'WHERE', 'GROUP', 'HAVING', 'ORDER', 'LIMIT', 'OFFSET', 'UNION', 'INTERSECT', 'EXCEPT', 'FOR', 'RETURNING'] as $keyword) {
            if ($token->isKeyword($keyword)) {
                return true;
            }
        }

        return false;
    }

    private function isOpeningParenthesis(SqlToken $token, SqlToken $referenceToken): bool
    {
        return $token->text === '(' && $this->sameLevel($token, $referenceToken);
    }

    /** @param list<SqlToken> $tokens */
    private function closingParenthesisIndex(array $tokens, int $openIndex): ?int
    {
        $open = $tokens[$openIndex] ?? null;
        if ($open === null) {
            return null;
        }
        $afterOpen = false;
        foreach ($tokens as $index => $token) {
            if ($token === $open) {
                $afterOpen = true;
            } elseif ($afterOpen && $token->text === ')' && $this->sameLevel($token, $open)) {
                return $index;
            }
        }

        return null;
    }

    /** @param list<SqlToken> $tokens */
    private function tokenAfter(array $tokens, SqlToken $referenceToken): SqlToken
    {
        $afterReference = false;
        foreach ($tokens as $token) {
            if ($afterReference) {
                return $token;
            }
            $afterReference = $token === $referenceToken;
        }

        return $referenceToken;
    }

    private function sameLevel(SqlToken $token, SqlToken $referenceToken): bool
    {
        return $token->depth === $referenceToken->depth
            && $token->bracketDepth === $referenceToken->bracketDepth;
    }
}
