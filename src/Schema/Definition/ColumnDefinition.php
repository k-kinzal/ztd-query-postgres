<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Schema\Definition;

use ZtdQuery\Platform\Postgres\Schema\PgSqlColumnTypeMapper;
use ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile;
use ZtdQuery\Schema\ColumnDeclaration;
use ZtdQuery\Sql\SqlTokenStream;

/**
 * Column definition operations for PostgreSQL definition.
 *
 * @visibility root
 */
final class ColumnDefinition
{
    /**
     * @return array{name: string, type: string, columnType: ColumnDeclaration, notNull: bool, primaryKey: bool, unique: bool, default: string|null, identity: bool, generatedExpression: string|null}|null
     */
    public function parseColumnDefinition(string $entry): ?array
    {
        if (preg_match('/^("[^"]+"|[a-zA-Z_]\w*)\s+(.+)$/is', $entry, $m) !== 1) {
            return null;
        }

        $name = (new TableConstraint())->unquoteIdentifier($m[1]);
        $rest = trim($m[2]);

        $typeInfo = (new ColumnTypeDeclaration())->extractType($rest);
        if ($typeInfo === null) {
            return null;
        }

        $nativeType = $typeInfo['type'];
        $afterType = $typeInfo['rest'];

        $notNull = preg_match('/\bNOT\s+NULL\b/i', $afterType) === 1;
        $primaryKey = preg_match('/\bPRIMARY\s+KEY\b/i', $afterType) === 1;
        $unique = preg_match('/\bUNIQUE\b/i', $afterType) === 1;
        $default = SqlTokenStream::tokenize($afterType, PgSqlLexerProfile::create())->topLevelClause(
            ['DEFAULT'],
            [
                ['NOT', 'NULL'], ['PRIMARY', 'KEY'], ['UNIQUE'], ['CHECK'],
                ['REFERENCES'], ['COLLATE'], ['CONSTRAINT'], ['GENERATED'], ['DEFERRABLE'],
            ],
        );
        $identity = self::isSerialType($nativeType)
            || ($default !== null && self::isSequenceDefault($default))
            || self::hasGeneratedIdentity($afterType);
        $generatedExpression = $this->generatedExpression($afterType, $identity);

        $normalizedType = str_contains($nativeType, '"') ? $nativeType : strtoupper($nativeType);
        $columnType = (new PgSqlColumnTypeMapper())->map($normalizedType);

        return [
            'name' => $name,
            'type' => $normalizedType,
            'columnType' => $columnType,
            'notNull' => $notNull,
            'primaryKey' => $primaryKey,
            'unique' => $unique,
            'default' => $default,
            'identity' => $identity,
            'generatedExpression' => $generatedExpression,
        ];
    }

    /**
     * Is sequence default.
     */
    public static function isSequenceDefault(string $expression): bool
    {
        foreach (SqlTokenStream::tokenize($expression, PgSqlLexerProfile::create())->significantTokens() as $token) {
            if ($token->text === '(') {
                continue;
            }

            return $token->isKeyword('NEXTVAL');
        }

        return false;
    }

    /**
     * Is serial type.
     */
    public static function isSerialType(string $nativeType): bool
    {
        foreach (['SMALLSERIAL', 'SERIAL', 'BIGSERIAL'] as $serialType) {
            if (strcasecmp($nativeType, $serialType) === 0) {
                return true;
            }
        }

        return false;
    }

    /**
     * Has generated identity.
     */
    public static function hasGeneratedIdentity(string $constraints): bool
    {
        $tokens = SqlTokenStream::tokenize($constraints, PgSqlLexerProfile::create())->significantTokens();
        $sequences = [
            ['GENERATED', 'ALWAYS', 'AS', 'IDENTITY'],
            ['GENERATED', 'BY', 'DEFAULT', 'AS', 'IDENTITY'],
        ];
        foreach ($tokens as $index => $token) {
            foreach ($sequences as $sequence) {
                foreach ($sequence as $relative => $keyword) {
                    $candidate = $tokens[$index + $relative] ?? null;
                    if ($candidate === null || !$candidate->isTopLevel() || !$candidate->isKeyword($keyword)) {
                        continue 2;
                    }
                }

                return true;
            }
        }

        return false;
    }
    /**
     * Reads a stored generated expression only for non-identity columns.
     */
    public function generatedExpression(string $afterType, bool $identity): ?string
    {
        $generatedExpression = null;
        if (!$identity) {
            $generatedExpression = SqlTokenStream::tokenize($afterType, PgSqlLexerProfile::create())->topLevelClause(
                ['GENERATED', 'ALWAYS', 'AS'],
                [['STORED']],
            );
            if ($generatedExpression === '') {
                $generatedExpression = null;
            }
        }

        return $generatedExpression;
    }
}
