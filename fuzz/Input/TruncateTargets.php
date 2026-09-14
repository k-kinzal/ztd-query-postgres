<?php

declare(strict_types=1);

namespace Fuzz\Input;

use ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile;
use ZtdQuery\Sql\SqlTokenStream;

/**
 * Counts the explicit target list independently of mutation construction.
 */
final class TruncateTargets
{
    /**
     * Target count.
     */
    public function targetCount(string $sql): ?int
    {
        $stream = SqlTokenStream::tokenize($sql, PgSqlLexerProfile::create());
        if ($stream->firstTopLevelKeyword() !== 'TRUNCATE') {
            return null;
        }
        $targetList = $stream->topLevelClause(['TRUNCATE'], [['RESTART', 'IDENTITY'], ['CONTINUE', 'IDENTITY'], ['CASCADE'], ['RESTRICT']]);
        if ($targetList === null || $targetList === '') {
            return 0;
        }
        return count(SqlTokenStream::tokenize($targetList, PgSqlLexerProfile::create())->splitTopLevel());
    }
}
