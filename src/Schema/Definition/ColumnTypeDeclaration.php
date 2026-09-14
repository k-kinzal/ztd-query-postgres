<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Schema\Definition;

use ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile;
use ZtdQuery\Sql\SqlToken;
use ZtdQuery\Sql\SqlTokenKind;
use ZtdQuery\Sql\SqlTokenStream;

/**
 * Column type declaration operations for PostgreSQL definition.
 *
 * @visibility root
 */
final class ColumnTypeDeclaration
{
    private const MULTI_WORD_TYPE_PREFIXES = [
        'DOUBLE', 'CHARACTER', 'TIME', 'TIMESTAMP',
        'BIT', 'INTERVAL',
    ];

    private const CONSTRAINT_KEYWORDS = [
        'PRIMARY', 'NOT', 'NULL', 'UNIQUE', 'CHECK', 'DEFAULT',
        'REFERENCES', 'COLLATE', 'CONSTRAINT', 'GENERATED', 'DEFERRABLE',
    ];
    /**
     * @return array{type: string, rest: string}|null
     */
    public function extractType(string $str): ?array
    {
        $str = ltrim($str);

        $quotedType = $this->extractQuotedType($str);
        if ($quotedType !== null) {
            return $quotedType;
        }

        $name = $this->typeName($str);
        if ($name === null) {
            return null;
        }
        $baseType = $name['type'];
        $rest = $name['rest'];

        $params = '';
        $trimmedRest = ltrim($rest);
        if (str_starts_with($trimmedRest, '(')) {
            if (preg_match('/^(\([^)]*\))/', $trimmedRest, $pm) === 1) {
                $params = $pm[1];
                $rest = substr($trimmedRest, strlen($params));
            }
        } else {
            $rest = $trimmedRest;
        }

        $arrayBrackets = '';
        $trimmedRest = ltrim($rest);
        while (preg_match('/^\[\s*\]/', $trimmedRest, $ab) === 1) {
            $arrayBrackets .= $ab[0];
            $trimmedRest = ltrim(substr($trimmedRest, strlen($ab[0])));
        }

        $fullType = $baseType . $params . $arrayBrackets;

        return ['type' => $fullType, 'rest' => trim($trimmedRest)];
    }

    /**
     * @return array{type: string, rest: string}|null
     */
    public function extractQuotedType(string $str): ?array
    {
        $tokens = SqlTokenStream::tokenize($str, PgSqlLexerProfile::create())->significantTokens();
        $first = $tokens[0] ?? null;
        if ($first === null) {
            return null;
        }
        if ($first->kind !== SqlTokenKind::QuotedIdentifier) {
            return null;
        }

        $last = $first;
        $index = 1;
        $separator = $tokens[$index] ?? null;
        if ($separator !== null && $separator->text === '.') {
            $index++;
            $qualifiedName = $tokens[$index] ?? null;
            if ($qualifiedName === null) {
                return null;
            }
            if (!self::isTypeIdentifier($qualifiedName)) {
                return null;
            }
            if ($qualifiedName->kind === SqlTokenKind::Word) {
                if (in_array(strtoupper($qualifiedName->text), self::CONSTRAINT_KEYWORDS, true)) {
                    return null;
                }
            }
            $last = $qualifiedName;
            $index++;
        }

        while (isset($tokens[$index]) && $tokens[$index]->text === '[') {
            $index++;
            $arrayClosing = $tokens[$index] ?? null;
            if ($arrayClosing === null) {
                return null;
            }
            if ($arrayClosing->text !== ']') {
                return null;
            }
            $last = $arrayClosing;
            $index++;
        }

        return [
            'type' => substr($str, 0, $last->endOffset()),
            'rest' => substr($str, $last->endOffset()),
        ];
    }

    /**
     * Is type identifier.
     */
    public static function isTypeIdentifier(SqlToken $token): bool
    {
        return in_array($token->kind, [SqlTokenKind::Word, SqlTokenKind::QuotedIdentifier], true);
    }
    /**
     * Reads a native type name and its optional second word before constraints.
     * @return array{type: string, rest: string}|null
     */
    public function typeName(string $str): ?array
    {
        if (preg_match('/^([a-zA-Z_]\w*)/i', $str, $m) !== 1) {
            return null;
        }

        $baseType = $m[1];
        $pos = strlen($baseType);
        $rest = substr($str, $pos);

        if (in_array(strtoupper($baseType), self::MULTI_WORD_TYPE_PREFIXES, true)) {
            $trimmedRest = ltrim($rest);
            if (preg_match('/^([a-zA-Z_]\w*)/i', $trimmedRest, $m2) === 1) {
                $secondWord = $m2[1];
                if (!in_array(strtoupper($secondWord), self::CONSTRAINT_KEYWORDS, true)) {
                    $baseType .= ' ' . $secondWord;
                    $pos = strlen($str) - strlen($trimmedRest) + strlen($secondWord);
                    $rest = substr($str, $pos);
                }
            }
        }

        return ['type' => $baseType, 'rest' => $rest];
    }
}
