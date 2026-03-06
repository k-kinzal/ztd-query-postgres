<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres;

use ZtdQuery\Platform\ErrorClassifier;
use ZtdQuery\Connection\Exception\DatabaseException;

/**
 * PostgreSQL-specific error classifier.
 *
 * Classifies PostgreSQL SQLSTATE codes to determine the type of error.
 * PostgreSQL uses 5-character SQLSTATE codes; driver error codes are mapped from these.
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
