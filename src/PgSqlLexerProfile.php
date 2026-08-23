<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres;

use ZtdQuery\Sql\SqlLexerProfile;

final class PgSqlLexerProfile
{
    public static function create(): SqlLexerProfile
    {
        return new SqlLexerProfile(
            lineCommentPrefixes: ['--'],
            whitespaceDelimitedLineCommentPrefixes: [],
            blockCommentPairs: ['/*' => '*/'],
            stringQuotePairs: ["'" => "'"],
            identifierQuotePairs: ['"' => '"'],
            namedParameterSeparators: [':' => []],
            namedParameterSuffixPatterns: [],
            namedParameterForbiddenPredecessors: [':' => [':']],
            backslashEscapedStringPrefixes: ['E', 'e'],
            positionalParameterPatterns: ['/^\$[0-9]+/', '/^\?/'],
            dollarQuoteDelimiterPattern: '/^\$(?:[_A-Za-z\x80-\xFF][_A-Za-z0-9\x80-\xFF]*)?\$/',
            numericLiteralPattern: '/^(?:0[xX]_?[0-9A-Fa-f](?:_?[0-9A-Fa-f])*|0[oO]_?[0-7](?:_?[0-7])*|0[bB]_?[01](?:_?[01])*|(?:(?:[0-9](?:_?[0-9])*)(?:\.(?:[0-9](?:_?[0-9])*)?)?|\.(?:[0-9](?:_?[0-9])*))(?:[eE][+-]?[0-9](?:_?[0-9])*)?)/',
            identifierStartPattern: '/^[_A-Za-z\x80-\xFF]$/',
            identifierPartPattern: '/^[_A-Za-z0-9$\x80-\xFF]$/',
            bracketPair: ['[', ']'],
            nestingPair: ['(', ')'],
            statementDelimiter: ';',
            listDelimiter: ',',
            nestedBlockComments: true,
            backslashEscapedStrings: false,
        );
    }
}
