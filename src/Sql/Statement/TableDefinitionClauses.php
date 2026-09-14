<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Sql\Statement;

use ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile;
use ZtdQuery\Platform\Postgres\Sql\PostgreSqlLexicalMasker;
use ZtdQuery\Sql\SqlTokenStream;

/**
 * Table definition clauses operations for PostgreSQL statement.
 *
 * @visibility root
 */
final class TableDefinitionClauses
{
    /**
     * @return list<string>
     */
    public function extractTruncateTables(string $sql): array
    {
        $stream = SqlTokenStream::tokenize($sql, PgSqlLexerProfile::create());
        $tokens = $stream->significantTokens();
        if ($stream->firstTopLevelKeyword() !== 'TRUNCATE') {
            return [];
        }

        $index = 1;
        $tableKeyword = $tokens[$index] ?? null;
        if ($tableKeyword !== null && $tableKeyword->isKeyword('TABLE')) {
            $index++;
        }

        $tableNames = [];
        while (isset($tokens[$index])) {
            if ($tokens[$index]->isKeyword('ONLY')) {
                $index++;
            }

            $identifier = (new Identifiers())->truncateIdentifierAt($stream, $index);
            if ($identifier === null) {
                break;
            }
            $tableName = $identifier['name'];
            $index = $identifier['next'];

            while (($tokens[$index] ?? null)?->text === '.') {
                $identifier = (new Identifiers())->truncateIdentifierAt($stream, $index + 1);
                if ($identifier === null) {
                    break 2;
                }
                $tableName = $identifier['name'];
                $index = $identifier['next'];
            }

            if (($tokens[$index] ?? null)?->text === '*') {
                $index++;
            }
            $tableNames[] = $tableName;

            if (($tokens[$index] ?? null)?->text !== ',') {
                break;
            }
            $index++;
        }

        return $tableNames;
    }

    /**
     * Extract table name from CREATE TABLE statement.
     */
    public function extractCreateTableName(string $sql): ?string
    {
        $sql = PostgreSqlLexicalMasker::maskComments($sql);
        if (preg_match('/CREATE\s+(?:TEMPORARY\s+|TEMP\s+|UNLOGGED\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?("[^"]+"|[a-zA-Z_]\w*(?:\."[^"]+"|\.(?:[a-zA-Z_]\w*))?)/i', $sql, $m) === 1) {
            return (new Identifiers())->unquoteIdentifier((new Identifiers())->stripSchemaPrefix($m[1]));
        }

        return null;
    }

    /**
     * Check if CREATE TABLE has IF NOT EXISTS.
     */
    public function hasIfNotExists(string $sql): bool
    {
        $sql = PostgreSqlLexicalMasker::maskComments($sql);
        return preg_match('/CREATE\s+(?:TEMPORARY\s+|TEMP\s+|UNLOGGED\s+)?TABLE\s+IF\s+NOT\s+EXISTS\b/i', $sql) === 1;
    }

    /**
     * Check if CREATE TABLE has AS SELECT.
     */
    public function hasCreateTableAsSelect(string $sql): bool
    {
        $sql = PostgreSqlLexicalMasker::maskComments($sql);
        return preg_match('/CREATE\s+(?:TEMPORARY\s+|TEMP\s+|UNLOGGED\s+)?TABLE\s+.*?\bAS\s+SELECT\b/is', $sql) === 1;
    }

    /**
     * Extract the SELECT SQL from CREATE TABLE ... AS SELECT.
     */
    public function extractCreateTableSelectSql(string $sql): ?string
    {
        $sql = PostgreSqlLexicalMasker::maskComments($sql);
        if (preg_match('/\bAS\s+(SELECT\b.+)$/is', $sql, $m) === 1) {
            return trim($m[1]);
        }

        return null;
    }

    /**
     * Check if CREATE TABLE has LIKE clause.
     */
    public function hasCreateTableLike(string $sql): bool
    {
        $sql = PostgreSqlLexicalMasker::maskComments($sql);
        return preg_match('/CREATE\s+(?:TEMPORARY\s+|TEMP\s+|UNLOGGED\s+)?TABLE\s+.*?\(\s*LIKE\s+/is', $sql) === 1;
    }

    /**
     * Extract the LIKE source table name.
     */
    public function extractCreateTableLikeSource(string $sql): ?string
    {
        $sql = PostgreSqlLexicalMasker::maskComments($sql);
        if (preg_match('/\(\s*LIKE\s+("[^"]+"|[a-zA-Z_]\w*(?:\."[^"]+"|\.(?:[a-zA-Z_]\w*))?)/i', $sql, $m) === 1) {
            return (new Identifiers())->unquoteIdentifier((new Identifiers())->stripSchemaPrefix($m[1]));
        }

        return null;
    }

    /**
     * Extract table name from DROP TABLE statement.
     */
    public function extractDropTableName(string $sql): ?string
    {
        $sql = PostgreSqlLexicalMasker::maskComments($sql);
        if (preg_match('/DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?("[^"]+"|[a-zA-Z_]\w*(?:\."[^"]+"|\.(?:[a-zA-Z_]\w*))?)/i', $sql, $m) === 1) {
            return (new Identifiers())->unquoteIdentifier((new Identifiers())->stripSchemaPrefix($m[1]));
        }

        return null;
    }

    /**
     * Check if DROP TABLE has IF EXISTS.
     */
    public function hasDropTableIfExists(string $sql): bool
    {
        $sql = PostgreSqlLexicalMasker::maskComments($sql);
        return preg_match('/DROP\s+TABLE\s+IF\s+EXISTS\b/i', $sql) === 1;
    }

    /**
     * Extract table name from ALTER TABLE statement.
     */
    public function extractAlterTableName(string $sql): ?string
    {
        $sql = PostgreSqlLexicalMasker::maskComments($sql);
        if (preg_match('/ALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?:ONLY\s+)?("[^"]+"|[a-zA-Z_]\w*(?:\."[^"]+"|\.(?:[a-zA-Z_]\w*))?)/i', $sql, $m) === 1) {
            return (new Identifiers())->unquoteIdentifier((new Identifiers())->stripSchemaPrefix($m[1]));
        }

        return null;
    }
}
