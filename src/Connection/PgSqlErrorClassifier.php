<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres\Connection;

use ZtdQuery\Connection\Exception\DatabaseException;
use ZtdQuery\Platform\ErrorClassifier;

/**
 * PostgreSQL-specific error classifier.
 *
 * Classifies PostgreSQL SQLSTATE codes to determine the type of error.
 * PostgreSQL uses 5-character SQLSTATE codes; driver error codes are mapped from these.
 *
 * @visibility public
 * @example Distinguish schema errors from syntax errors
 *     $classifier = new \ZtdQuery\Platform\Postgres\Connection\PgSqlErrorClassifier();
 *     $classifier->isUnknownSchemaError(new \ZtdQuery\Connection\Exception\DatabaseException('42P01: relation "users" does not exist')) // => true
 *     $classifier->isUnknownSchemaError(new \ZtdQuery\Connection\Exception\DatabaseException('42601: syntax error')) // => false
 */
final class PgSqlErrorClassifier implements ErrorClassifier
{
    /**
     * Known PostgreSQL error codes for unknown schema errors.
     * These are driver-specific integer codes that PDO maps from SQLSTATE.
     */
    private const SCHEMA_ERROR_CODES = [
        7, // Generic PDO pgsql driver error code for query failures
    ];

    /**
     * Known SQLSTATE codes for schema errors.
     */
    private const SCHEMA_SQLSTATES = [
        '42703', // undefined_column
        '42P01', // undefined_table
        '42P02', // undefined_parameter
        '42P10', // invalid_column_reference
        '42704', // undefined_object
    ];

    /**
     * {@inheritDoc}
     * @visibility public
     * @example Distinguish schema errors from syntax errors
     *     $classifier = new \ZtdQuery\Platform\Postgres\Connection\PgSqlErrorClassifier();
     *     $classifier->isUnknownSchemaError(new \ZtdQuery\Connection\Exception\DatabaseException('42P01: relation "users" does not exist')) // => true
     *     $classifier->isUnknownSchemaError(new \ZtdQuery\Connection\Exception\DatabaseException('42601: syntax error')) // => false
     */
    public function isUnknownSchemaError(DatabaseException $e): bool
    {
        $code = $e->getDriverErrorCode();

        $message = $e->getMessage();
        foreach (self::SCHEMA_SQLSTATES as $sqlstate) {
            if (str_contains($message, $sqlstate)) {
                return true;
            }
        }

        if (preg_match('/column ".*" does not exist/i', $message) === 1) {
            return true;
        }
        if (preg_match('/relation ".*" does not exist/i', $message) === 1) {
            return true;
        }
        if (preg_match('/table ".*" does not exist/i', $message) === 1) {
            return true;
        }

        if ($code === null) {
            return false;
        }

        return in_array($code, self::SCHEMA_ERROR_CODES, true);
    }
}
