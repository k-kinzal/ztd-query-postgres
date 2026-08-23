<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres;

use ZtdQuery\Exception\UnsupportedSqlException;
use ZtdQuery\Sql\SqlToken;
use ZtdQuery\Sql\SqlTokenKind;
use ZtdQuery\Sql\SqlTokenStream;

final class PgSqlMergeParser
{
    private PgSqlCteShadowComposer $cteComposer;

    public function __construct()
    {
        $this->cteComposer = new PgSqlCteShadowComposer();
    }

    public function parse(string $sql): PgSqlMergeStatement
    {
        $statementSql = $this->cteComposer->statementSql($sql);
        $tokens = SqlTokenStream::tokenize($statementSql, PgSqlLexerProfile::create())->significantTokens();
        $index = 0;
        $first = $tokens[$index] ?? null;
        if ($first === null) {
            throw new UnsupportedSqlException($sql, 'Malformed MERGE statement');
        }
        if (!$first->isKeyword('MERGE')) {
            throw new UnsupportedSqlException($sql, 'Malformed MERGE statement');
        }
        $index++;
        $next = $tokens[$index] ?? null;
        if ($next !== null && $next->isKeyword('INTO')) {
            $index++;
            $next = $tokens[$index] ?? null;
        }
        if ($next !== null && $next->isKeyword('ONLY')) {
            $index++;
        }

        $target = $this->relationAt($statementSql, $tokens, $index);
        if ($target === null) {
            throw new UnsupportedSqlException($sql, 'Cannot resolve MERGE target');
        }
        $index = $target['next'];
        if ($this->isSymbol($tokens[$index] ?? null, '*')) {
            $target['last'] = $tokens[$index];
            $index++;
        }

        $usingIndex = $this->keywordIndexAfter($tokens, 'USING', $target['last']);
        if ($usingIndex === null) {
            throw new UnsupportedSqlException($sql, 'MERGE requires USING');
        }
        $targetAlias = $this->targetAlias($sql, $tokens, $index, $usingIndex, $target['name']);

        $usingToken = $tokens[$usingIndex];
        $whenTokens = $this->mergeWhenTokens($tokens);
        $firstWhen = $whenTokens[0] ?? null;
        if ($firstWhen === null) {
            throw new UnsupportedSqlException($sql, 'MERGE requires a WHEN clause');
        }
        $onToken = $this->lastKeywordBetween($tokens, 'ON', $usingToken, $firstWhen);
        if ($onToken === null) {
            throw new UnsupportedSqlException($sql, 'MERGE requires an ON condition');
        }

        $sourceSql = trim(substr(
            $statementSql,
            $usingToken->endOffset(),
            $onToken->offset - $usingToken->endOffset(),
        ));
        $joinConditionSql = trim(substr(
            $statementSql,
            $onToken->endOffset(),
            $firstWhen->offset - $onToken->endOffset(),
        ));
        if ($sourceSql === '') {
            throw new UnsupportedSqlException($sql, 'MERGE requires a source and join condition');
        }
        if ($joinConditionSql === '') {
            throw new UnsupportedSqlException($sql, 'MERGE requires a source and join condition');
        }

        $clauses = [];
        foreach ($whenTokens as $clauseIndex => $whenToken) {
            $end = isset($whenTokens[$clauseIndex + 1])
                ? $whenTokens[$clauseIndex + 1]->offset
                : strlen($statementSql);
            $clauseSql = substr($statementSql, $whenToken->offset, $end - $whenToken->offset);
            $clauses[] = $this->parseClause($sql, $clauseSql);
        }
        if ($clauses === []) {
            throw new UnsupportedSqlException($sql, 'MERGE requires a WHEN clause');
        }

        return new PgSqlMergeStatement(
            $target['name'],
            $target['sql'],
            $targetAlias,
            $sourceSql,
            $joinConditionSql,
            $clauses,
        );
    }

    /**
     * @param list<SqlToken> $tokens
     * @return array{name: string, sql: string, next: int, last: SqlToken}|null
     */
    private function relationAt(string $sql, array $tokens, int $index): ?array
    {
        $first = $tokens[$index] ?? null;
        $name = $first !== null ? $this->identifierName($first) : null;
        if ($first === null || $name === null) {
            return null;
        }

        $start = $first->offset;
        $end = $first->endOffset();
        $last = $first;
        $index++;
        while ($this->isSymbol($tokens[$index] ?? null, '.')) {
            $component = $tokens[$index + 1] ?? null;
            if ($component === null) {
                return null;
            }
            $componentName = $this->identifierName($component);
            if ($componentName === null) {
                return null;
            }
            $name = $componentName;
            $end = $component->endOffset();
            $last = $component;
            $index += 2;
        }

        return [
            'name' => $name,
            'sql' => substr($sql, $start, $end - $start),
            'next' => $index,
            'last' => $last,
        ];
    }

    /** @param list<SqlToken> $tokens */
    private function targetAlias(
        string $sql,
        array $tokens,
        int $start,
        int $end,
        string $default,
    ): string {
        if ($start === $end) {
            return $default;
        }
        if ($tokens[$start]->isKeyword('AS')) {
            $start++;
        }
        if ($start + 1 !== $end) {
            throw new UnsupportedSqlException($sql, 'Malformed MERGE target alias');
        }
        $alias = $this->identifierName($tokens[$start]);
        if ($alias === null) {
            throw new UnsupportedSqlException($sql, 'Malformed MERGE target alias');
        }

        return $alias;
    }

    private function parseClause(string $originalSql, string $clauseSql): PgSqlMergeClause
    {
        $clauseSql = rtrim($clauseSql, "; \t\n\r\0\x0B");
        $tokens = SqlTokenStream::tokenize($clauseSql, PgSqlLexerProfile::create())->significantTokens();
        $index = 1;
        $matchKind = PgSqlMergeMatchKind::Matched;
        $matchToken = $tokens[$index];
        if ($matchToken->isKeyword('NOT')) {
            $matchKind = PgSqlMergeMatchKind::NotMatched;
            $index++;
            $matchToken = $tokens[$index];
        }
        if (!$matchToken->isKeyword('MATCHED')) {
            throw new UnsupportedSqlException($originalSql, 'Malformed MERGE WHEN clause');
        }
        $index++;
        $modifier = $tokens[$index] ?? null;
        if ($modifier !== null && $modifier->isKeyword('BY')) {
            throw new UnsupportedSqlException($originalSql, 'MERGE BY SOURCE and BY TARGET are not supported');
        }

        $thenIndex = $this->keywordIndexOutsideCase($tokens, 'THEN');
        if ($thenIndex === null) {
            throw new UnsupportedSqlException($originalSql, 'MERGE WHEN clause requires THEN');
        }
        $conditionSql = null;
        if ($index !== $thenIndex) {
            if (!$tokens[$index]->isKeyword('AND')) {
                throw new UnsupportedSqlException($originalSql, 'Malformed MERGE WHEN condition');
            }
            $conditionSql = trim(substr(
                $clauseSql,
                $tokens[$index]->endOffset(),
                $tokens[$thenIndex]->offset - $tokens[$index]->endOffset(),
            ));
            if ($conditionSql === '') {
                throw new UnsupportedSqlException($originalSql, 'MERGE WHEN condition cannot be empty');
            }
        }

        $actionSql = substr($clauseSql, $tokens[$thenIndex]->endOffset());
        $actionTokens = SqlTokenStream::tokenize($actionSql, PgSqlLexerProfile::create())->significantTokens();
        $first = $actionTokens[0] ?? null;
        if ($first === null) {
            throw new UnsupportedSqlException($originalSql, 'MERGE action is not supported');
        }
        if ($first->isKeyword('DO')) {
            $second = $actionTokens[1] ?? null;
            if ($second !== null && $second->isKeyword('NOTHING')) {
                if (count($actionTokens) === 2) {
                    return new PgSqlMergeClause($matchKind, $conditionSql, PgSqlMergeActionKind::DoNothing);
                }
            }
        }
        if ($matchKind === PgSqlMergeMatchKind::Matched && $first->isKeyword('DELETE') && count($actionTokens) === 1) {
            return new PgSqlMergeClause($matchKind, $conditionSql, PgSqlMergeActionKind::Delete);
        }
        if ($matchKind === PgSqlMergeMatchKind::Matched && $first->isKeyword('UPDATE')) {
            return new PgSqlMergeClause(
                $matchKind,
                $conditionSql,
                PgSqlMergeActionKind::Update,
                $this->parseAssignments($originalSql, $actionSql, $actionTokens),
            );
        }
        if ($matchKind === PgSqlMergeMatchKind::NotMatched && $first->isKeyword('INSERT')) {
            $insert = $this->parseInsert($originalSql, $actionSql, $actionTokens);

            return new PgSqlMergeClause(
                $matchKind,
                $conditionSql,
                PgSqlMergeActionKind::Insert,
                [],
                $insert['columns'],
                $insert['values'],
            );
        }

        throw new UnsupportedSqlException($originalSql, 'MERGE action is not supported');
    }

    /**
     * @param list<SqlToken> $tokens
     * @return array<string, string>
     */
    private function parseAssignments(string $originalSql, string $actionSql, array $tokens): array
    {
        $set = $tokens[1] ?? null;
        if ($set === null) {
            throw new UnsupportedSqlException($originalSql, 'MERGE UPDATE requires SET');
        }
        if (!$set->isKeyword('SET')) {
            throw new UnsupportedSqlException($originalSql, 'MERGE UPDATE requires SET');
        }
        $setSql = substr($actionSql, $set->endOffset());
        $assignments = [];
        foreach (SqlTokenStream::tokenize($setSql, PgSqlLexerProfile::create())->splitTopLevel() as $assignmentSql) {
            $assignmentTokens = SqlTokenStream::tokenize($assignmentSql, PgSqlLexerProfile::create())->significantTokens();
            $equals = [];
            foreach ($assignmentTokens as $assignmentIndex => $token) {
                if ($this->isSymbol($token, '=') && $token->isTopLevel()) {
                    $equals[] = $assignmentIndex;
                }
            }
            if (count($equals) !== 1) {
                throw new UnsupportedSqlException($originalSql, 'MERGE UPDATE requires simple column assignments');
            }
            $equalsIndex = $equals[0];
            if ($equalsIndex !== 1 || count($assignmentTokens) < 3) {
                throw new UnsupportedSqlException($originalSql, 'MERGE UPDATE requires simple column assignments');
            }
            $column = $this->identifierName($assignmentTokens[0]);
            if ($column === null) {
                throw new UnsupportedSqlException($originalSql, 'MERGE UPDATE target must be a column');
            }
            $value = trim(substr($assignmentSql, $assignmentTokens[$equalsIndex]->endOffset()));
            if ($value === '') {
                throw new UnsupportedSqlException($originalSql, 'MERGE UPDATE value cannot be empty');
            }
            if (array_key_exists($column, $assignments)) {
                throw new UnsupportedSqlException($originalSql, 'MERGE UPDATE cannot assign a column more than once');
            }
            $assignments[$column] = $value;
        }
        if ($assignments === []) {
            throw new UnsupportedSqlException($originalSql, 'MERGE UPDATE requires assignments');
        }

        return $assignments;
    }

    /**
     * @param list<SqlToken> $tokens
     * @return array{columns: list<string>, values: list<string>}
     */
    private function parseInsert(string $originalSql, string $actionSql, array $tokens): array
    {
        $index = 1;
        $columns = [];
        if ($this->isSymbol($tokens[$index] ?? null, '(')) {
            $list = $this->parenthesizedList($originalSql, $actionSql, $tokens, $index);
            foreach ($list['items'] as $columnSql) {
                $columnTokens = SqlTokenStream::tokenize($columnSql, PgSqlLexerProfile::create())->significantTokens();
                $column = count($columnTokens) === 1 ? $this->identifierName($columnTokens[0]) : null;
                if ($column === null) {
                    throw new UnsupportedSqlException($originalSql, 'MERGE INSERT columns must be identifiers');
                }
                if (in_array($column, $columns, true)) {
                    throw new UnsupportedSqlException($originalSql, 'MERGE INSERT cannot name a column more than once');
                }
                $columns[] = $column;
            }
            $index = $list['next'];
        }

        $insertAction = $tokens[$index] ?? null;
        $afterAction = $tokens[$index + 1] ?? null;
        if ($insertAction !== null && $insertAction->isKeyword('DEFAULT')) {
            if ($afterAction === null || !$afterAction->isKeyword('VALUES') || $index + 2 !== count($tokens)) {
                throw new UnsupportedSqlException($originalSql, 'MERGE INSERT requires VALUES');
            }
            if ($columns !== []) {
                throw new UnsupportedSqlException($originalSql, 'MERGE INSERT DEFAULT VALUES cannot name columns');
            }

            return ['columns' => [], 'values' => []];
        }
        if ($insertAction === null) {
            throw new UnsupportedSqlException($originalSql, 'MERGE INSERT requires VALUES');
        }
        if (!$insertAction->isKeyword('VALUES')) {
            throw new UnsupportedSqlException($originalSql, 'MERGE INSERT requires VALUES');
        }
        if (!$this->isSymbol($afterAction, '(')) {
            throw new UnsupportedSqlException($originalSql, 'MERGE INSERT requires VALUES');
        }

        $values = $this->parenthesizedList($originalSql, $actionSql, $tokens, $index + 1);
        if ($values['next'] !== count($tokens)) {
            throw new UnsupportedSqlException($originalSql, 'Malformed MERGE INSERT values');
        }
        if ($columns !== [] && count($columns) !== count($values['items'])) {
            throw new UnsupportedSqlException($originalSql, 'MERGE INSERT values count does not match column count');
        }

        return ['columns' => $columns, 'values' => $values['items']];
    }

    /**
     * @param list<SqlToken> $tokens
     * @return array{items: list<string>, next: int}
     */
    private function parenthesizedList(
        string $originalSql,
        string $sql,
        array $tokens,
        int $openIndex,
    ): array {
        $open = $tokens[$openIndex];
        $afterOpen = false;
        foreach ($tokens as $closeIndex => $token) {
            if ($token === $open) {
                $afterOpen = true;
                continue;
            }
            if (!$afterOpen) {
                continue;
            }
            if (!$this->isSymbol($token, ')')) {
                continue;
            }
            if ($token->depth !== $open->depth) {
                continue;
            }
            $listSql = substr($sql, $open->endOffset(), $token->offset - $open->endOffset());
            $items = SqlTokenStream::tokenize($listSql, PgSqlLexerProfile::create())->splitTopLevel();

            return ['items' => $items, 'next' => $closeIndex + 1];
        }

        throw new UnsupportedSqlException($originalSql, 'Malformed MERGE parenthesized list');
    }

    /** @param list<SqlToken> $tokens */
    private function keywordIndexAfter(array $tokens, string $keyword, SqlToken $anchor): ?int
    {
        $afterAnchor = false;
        foreach ($tokens as $index => $token) {
            if ($token === $anchor) {
                $afterAnchor = true;
                continue;
            }
            if ($afterAnchor && $token->isTopLevel() && $token->isKeyword($keyword)) {
                return $index;
            }
        }

        return null;
    }

    /** @param list<SqlToken> $tokens */
    private function lastKeywordBetween(
        array $tokens,
        string $keyword,
        SqlToken $start,
        SqlToken $end,
    ): ?SqlToken {
        $found = null;
        $withinRange = false;
        foreach ($tokens as $token) {
            if ($token === $start) {
                $withinRange = true;
                continue;
            }
            if ($token === $end) {
                break;
            }
            if ($withinRange && $token->isTopLevel() && $token->isKeyword($keyword)) {
                $found = $token;
            }
        }

        return $found;
    }

    /**
     * @param list<SqlToken> $tokens
     * @return list<SqlToken>
     */
    private function mergeWhenTokens(array $tokens): array
    {
        $whenTokens = [];
        $caseDepth = 0;
        foreach ($tokens as $index => $token) {
            if (!$token->isTopLevel()) {
                continue;
            }
            if ($token->isKeyword('CASE')) {
                $caseDepth++;
                continue;
            }
            if ($token->isKeyword('END')) {
                if ($caseDepth !== 0) {
                    $caseDepth--;
                    continue;
                }
            }
            if ($caseDepth !== 0 || !$token->isKeyword('WHEN')) {
                continue;
            }
            $next = $tokens[$index + 1] ?? null;
            if ($next === null) {
                continue;
            }
            if ($next->isKeyword('MATCHED')) {
                $whenTokens[] = $token;
                continue;
            }
            if (!$next->isKeyword('NOT')) {
                continue;
            }
            $afterNext = $tokens[$index + 2] ?? null;
            if ($afterNext === null) {
                continue;
            }
            if ($afterNext->isKeyword('MATCHED')) {
                $whenTokens[] = $token;
            }
        }

        return $whenTokens;
    }

    /** @param list<SqlToken> $tokens */
    private function keywordIndexOutsideCase(array $tokens, string $keyword): ?int
    {
        $caseDepth = 0;
        foreach ($tokens as $index => $token) {
            if (!$token->isTopLevel()) {
                continue;
            }
            if ($token->isKeyword('CASE')) {
                $caseDepth++;
                continue;
            }
            if ($token->isKeyword('END')) {
                if ($caseDepth !== 0) {
                    $caseDepth--;
                    continue;
                }
            }
            if ($caseDepth === 0 && $token->isKeyword($keyword)) {
                return $index;
            }
        }

        return null;
    }

    private function identifierName(SqlToken $token): ?string
    {
        if ($token->kind === SqlTokenKind::Word) {
            return strtolower($token->text);
        }
        if ($token->kind !== SqlTokenKind::QuotedIdentifier || strlen($token->text) <= 2) {
            return null;
        }

        return str_replace('""', '"', substr($token->text, 1, -1));
    }

    private function isSymbol(?SqlToken $token, string $symbol): bool
    {
        if ($token === null) {
            return false;
        }
        return $token->text === $symbol;
    }
}
