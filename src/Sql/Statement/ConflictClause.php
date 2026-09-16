<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Sql\Statement;

use ZtdQuery\Platform\Postgres\Sql\Conflict\PgSqlConflictTarget;
use ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile;
use ZtdQuery\Sql\SqlToken;
use ZtdQuery\Sql\SqlTokenKind;
use ZtdQuery\Sql\SqlTokenStream;

/**
 * Conflict clause operations for PostgreSQL statement.
 *
 * @visibility root
 */
final class ConflictClause
{
    /**
     * Extract on conflict target.
     */
    public function extractOnConflictTarget(string $sql): ?PgSqlConflictTarget
    {
        $tokens = SqlTokenStream::tokenize($sql, PgSqlLexerProfile::create())->significantTokens();
        $conflict = self::findOnConflictTargetStart($tokens);
        if ($conflict === null) {
            return null;
        }

        $next = $tokens[$conflict] ?? null;
        if ($next === null) {
            return null;
        }
        if ($next->isKeyword('DO')) {
            return new PgSqlConflictTarget(false);
        }
        if ($next->isKeyword('ON')) {
            return $this->namedConstraint($tokens, $conflict);
        }
        if ($next->text !== '(') {
            return null;
        }

        $closing = self::findTopLevelSymbol($tokens, $conflict, ')');
        if ($closing === null) {
            return null;
        }

        $columnSql = substr(
            $sql,
            $next->endOffset(),
            $tokens[$closing]->offset - $next->endOffset(),
        );
        $columns = $this->indexColumns($columnSql);
        if ($columns === null) {
            return null;
        }

        return $this->indexTarget($sql, $tokens, $closing, $columns);
    }

    /**
     * @param list<SqlToken> $tokens
     */
    public static function findOnConflictTargetStart(array $tokens): ?int
    {
        foreach ($tokens as $index => $token) {
            if (!$token->isTopLevel()) {
                continue;
            }
            if (!$token->isKeyword('ON')) {
                continue;
            }
            $following = $tokens[$index + 1] ?? $token;
            if ($following->isKeyword('CONFLICT')) {
                return $index + 2;
            }
        }

        return null;
    }

    /**
     * @param list<SqlToken> $tokens
     */
    public static function findTopLevelSymbol(array $tokens, int $start, string $symbol): ?int
    {
        for ($index = $start, $count = count($tokens); $index < $count; $index++) {
            $token = $tokens[$index];
            if (!$token->isTopLevel()) {
                continue;
            }
            if ($token->kind !== SqlTokenKind::Symbol) {
                continue;
            }
            if ($token->text === $symbol) {
                return $index;
            }
        }

        return null;
    }

    /**
     * @param list<SqlToken> $tokens
     */
    public static function findTopLevelKeyword(array $tokens, int $start, string $keyword): ?int
    {
        for ($index = $start, $count = count($tokens); $index < $count; $index++) {
            $token = $tokens[$index];
            if ($token->isTopLevel() && $token->isKeyword($keyword)) {
                return $index;
            }
        }

        return null;
    }

    /**
     * Extract ON CONFLICT ... DO UPDATE SET columns and values.
     *
     * @return array{columns: list<string>, values: array<string, string>}
     */
    public function extractOnConflictUpdateColumns(string $sql): array
    {
        $columns = [];
        /**
         * @var array<string, string> $values
         */
        $values = [];

        $action = SqlTokenStream::tokenize($sql, PgSqlLexerProfile::create())->topLevelClause(['DO'], [['RETURNING']]);
        if ($action === null) {
            return ['columns' => [], 'values' => []];
        }
        $actionStream = SqlTokenStream::tokenize($action, PgSqlLexerProfile::create());
        if ($actionStream->firstTopLevelKeyword() !== 'UPDATE') {
            return ['columns' => [], 'values' => []];
        }
        $setClause = $actionStream->topLevelClause(['SET'], [['WHERE']]);
        if ($setClause === null) {
            return ['columns' => [], 'values' => []];
        }
        $setClause = rtrim($setClause, '; ');

        $assignments = SqlTokenStream::tokenize($setClause, PgSqlLexerProfile::create())->splitTopLevel();

        foreach ($assignments as $assignment) {
            $assignment = trim($assignment);
            if (preg_match('/^("[^"]+"|[a-zA-Z_]\w*)\s*=\s*(.+)$/s', $assignment, $parts) === 1) {
                $colName = (new \ZtdQuery\Platform\Postgres\Sql\Lexing\IdentifierDecoder())->unquoteIdentifier($parts[1]);
                $columns[] = $colName;
                $values[$colName] = trim($parts[2]);
            }
        }

        return ['columns' => $columns, 'values' => $values];
    }

    /**
     * Extract on conflict update where.
     */
    public function extractOnConflictUpdateWhere(string $sql): ?string
    {
        return SqlTokenStream::tokenize($sql, PgSqlLexerProfile::create())->topLevelClauseAfter(
            ['DO', 'UPDATE', 'SET'],
            ['WHERE'],
            [['RETURNING']],
        );
    }
    /**
     * Resolves an ON CONSTRAINT target after the conflict keyword.
     * @param list<SqlToken> $tokens
     */
    public function namedConstraint(array $tokens, int $conflict): ?PgSqlConflictTarget
    {
        $constraintKeyword = $tokens[$conflict + 1] ?? null;
        if ($constraintKeyword === null) {
            return null;
        }
        if (!$constraintKeyword->isKeyword('CONSTRAINT')) {
            return null;
        }
        $constraintToken = $tokens[$conflict + 2] ?? null;
        if ($constraintToken === null) {
            return null;
        }
        $identifier = SqlTokenStream::tokenize($constraintToken->text, PgSqlLexerProfile::create())->identifierAt();
        if ($identifier === null) {
            return null;
        }

        return new PgSqlConflictTarget(true, constraint: $identifier['name']);
    }

    /**
     * Requires each inferred-index item to consist of one identifier.
     * @return list<string>|null
     */
    public function indexColumns(string $columnSql): ?array
    {
        $columns = [];
        foreach (SqlTokenStream::tokenize($columnSql, PgSqlLexerProfile::create())->splitTopLevel() as $part) {
            $stream = SqlTokenStream::tokenize($part, PgSqlLexerProfile::create());
            $identifier = $stream->identifierAt();
            if ($identifier === null) {
                return null;
            }
            if ($identifier['next'] !== count($stream->significantTokens())) {
                return null;
            }
            $columns[] = $identifier['name'];
        }

        return $columns;
    }

    /**
     * Resolves the optional index predicate and requires an ON CONFLICT action.
     * @param list<SqlToken> $tokens
     * @param list<string> $columns
     */
    public function indexTarget(string $sql, array $tokens, int $closing, array $columns): ?PgSqlConflictTarget
    {
        $do = self::findTopLevelKeyword($tokens, $closing, 'DO');
        if ($do === null) {
            return null;
        }

        $predicate = null;
        $afterColumns = $tokens[$closing + 1] ?? null;
        if ($afterColumns !== null) {
            if ($afterColumns->isKeyword('WHERE')) {
                $predicate = trim(substr(
                    $sql,
                    $afterColumns->endOffset(),
                    $tokens[$do]->offset - $afterColumns->endOffset(),
                ));
                if ($predicate === '') {
                    return null;
                }
            }
        }

        return new PgSqlConflictTarget(true, $columns, $predicate);
    }

    /**
     * Check if INSERT has ON CONFLICT clause.
     */
    public function hasOnConflict(string $sql): bool
    {
        $sql = \ZtdQuery\Platform\Postgres\Sql\PostgreSqlLexicalMasker::maskComments($sql);
        return preg_match('/\bON\s+CONFLICT\b/i', $sql) === 1;
    }
}
