<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Sql\Transaction;

use ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile;
use ZtdQuery\Sql\SqlTokenStream;
use ZtdQuery\Sql\TransactionStatement;
use ZtdQuery\Sql\TransactionStatementParser;

/**
 * Transaction statement parser for PostgreSQL queries.
 */
final class PgSqlTransactionStatementParser implements TransactionStatementParser
{
    /**
     * Parses PostgreSQL SQL into the supported structural representation.
     */
    public function parse(string $sql): ?TransactionStatement
    {
        $tokens = SqlTokenStream::tokenize($sql, PgSqlLexerProfile::create())->significantTokens();
        if (($tokens[count($tokens) - 1] ?? null)?->text === ';') {
            array_pop($tokens);
        }
        if ((new KeywordForm())->matchesAny($tokens, [['BEGIN'], ['BEGIN', 'WORK'], ['BEGIN', 'TRANSACTION'], ['START', 'TRANSACTION']])) {
            return TransactionStatement::begin();
        }
        if ((new KeywordForm())->matchesAny($tokens, [['COMMIT'], ['COMMIT', 'WORK'], ['COMMIT', 'TRANSACTION'], ['END'], ['END', 'WORK'], ['END', 'TRANSACTION']])) {
            return TransactionStatement::commit();
        }
        if ((new KeywordForm())->matchesAny($tokens, [['ROLLBACK'], ['ROLLBACK', 'WORK'], ['ROLLBACK', 'TRANSACTION']])) {
            return TransactionStatement::rollback();
        }
        $name = (new KeywordForm())->nameAfter($tokens, [['SAVEPOINT']]);
        if ($name !== null) {
            return TransactionStatement::savepoint($name);
        }
        $name = (new KeywordForm())->nameAfter($tokens, [['ROLLBACK', 'TO'], ['ROLLBACK', 'TO', 'SAVEPOINT']]);
        if ($name !== null) {
            return TransactionStatement::rollbackTo($name);
        }
        $name = (new KeywordForm())->nameAfter($tokens, [['RELEASE'], ['RELEASE', 'SAVEPOINT']]);

        return $name !== null ? TransactionStatement::release($name) : null;
    }
}
