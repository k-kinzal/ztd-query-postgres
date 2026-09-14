<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Sql\Merge;

use ZtdQuery\Exception\UnsupportedSqlException;
use ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile;
use ZtdQuery\Sql\SqlToken;
use ZtdQuery\Sql\SqlTokenStream;

/**
 * Action clause operations for PostgreSQL merge.
 *
 * @visibility root
 */
final class ActionClause
{
    /**
     * Parse clause.
     * @throws UnsupportedSqlException
     */
    public function parseClause(string $originalSql, string $clauseSql): PgSqlMergeClause
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

        $thenIndex = (new BranchTokens())->keywordIndexOutsideCase($tokens, 'THEN');
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
        return $this->action($originalSql, $actionSql, $matchKind, $conditionSql);
    }

    /**
     * @param list<SqlToken> $tokens
     * @return array<string, string>
     * @throws UnsupportedSqlException
     */
    public function parseAssignments(string $originalSql, string $actionSql, array $tokens): array
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
                if ((new RelationTarget())->isSymbol($token, '=') && $token->isTopLevel()) {
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
            $column = (new RelationTarget())->identifierName($assignmentTokens[0]);
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
     * @throws UnsupportedSqlException
     */
    public function parseInsert(string $originalSql, string $actionSql, array $tokens): array
    {
        $index = 1;
        $columns = [];
        if ((new RelationTarget())->isSymbol($tokens[$index] ?? null, '(')) {
            $list = $this->parenthesizedList($originalSql, $actionSql, $tokens, $index);
            $columns = $this->insertColumns($originalSql, $list['items']);
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
        if (!(new RelationTarget())->isSymbol($afterAction, '(')) {
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
     * @throws UnsupportedSqlException
     */
    public function parenthesizedList(
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
            if (!(new RelationTarget())->isSymbol($token, ')')) {
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
    /**
     * Resolves the action permitted by a MATCHED or NOT MATCHED branch.
     * @throws UnsupportedSqlException
     */
    public function action(string $originalSql, string $actionSql, PgSqlMergeMatchKind $matchKind, ?string $conditionSql): PgSqlMergeClause
    {
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
     * Validates the unique identifier list in a MERGE INSERT action.
     * @param list<string> $items
     * @return list<string>
     * @throws UnsupportedSqlException
     */
    public function insertColumns(string $originalSql, array $items): array
    {
        $columns = [];
        foreach ($items as $columnSql) {
            $columnTokens = SqlTokenStream::tokenize($columnSql, PgSqlLexerProfile::create())->significantTokens();
            $column = count($columnTokens) === 1 ? (new RelationTarget())->identifierName($columnTokens[0]) : null;
            if ($column === null) {
                throw new UnsupportedSqlException($originalSql, 'MERGE INSERT columns must be identifiers');
            }
            if (in_array($column, $columns, true)) {
                throw new UnsupportedSqlException($originalSql, 'MERGE INSERT cannot name a column more than once');
            }
            $columns[] = $column;
        }
        return $columns;
    }
}
