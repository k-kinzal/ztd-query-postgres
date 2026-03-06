<?php

declare(strict_types=1);

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use ZtdQuery\Connection\Exception\DatabaseException;
use ZtdQuery\Platform\Postgres\PgSqlErrorClassifier;
use PHPUnit\Framework\Attributes\CoversClass;

#[CoversClass(PgSqlErrorClassifier::class)]
final class PgSqlErrorClassifierTest extends TestCase
{
    public function testUndefinedColumnError(): void
    {
        $classifier = new PgSqlErrorClassifier();
        $e = new DatabaseException('ERROR: column "unknown_col" does not exist (42703)', 7);
        self::assertTrue($classifier->isUnknownSchemaError($e));
    }

    public function testUndefinedTableError(): void
    {
        $classifier = new PgSqlErrorClassifier();
        $e = new DatabaseException('ERROR: relation "unknown_table" does not exist (42P01)', 7);
        self::assertTrue($classifier->isUnknownSchemaError($e));
    }

    public function testColumnDoesNotExistPattern(): void
    {
        $classifier = new PgSqlErrorClassifier();
        $e = new DatabaseException('column "foo" does not exist', null);
        self::assertTrue($classifier->isUnknownSchemaError($e));
    }

    public function testRelationDoesNotExistPattern(): void
    {
        $classifier = new PgSqlErrorClassifier();
        $e = new DatabaseException('relation "bar" does not exist', null);
        self::assertTrue($classifier->isUnknownSchemaError($e));
    }

    public function testSyntaxErrorNotSchemaError(): void
    {
        $classifier = new PgSqlErrorClassifier();
        $e = new DatabaseException('ERROR: syntax error at or near "SELEC" (42601)', null);
        self::assertFalse($classifier->isUnknownSchemaError($e));
    }

    public function testConstraintViolationNotSchemaError(): void
    {
        $classifier = new PgSqlErrorClassifier();
        $e = new DatabaseException('ERROR: duplicate key value violates unique constraint (23505)', null);
        self::assertFalse($classifier->isUnknownSchemaError($e));
    }

    public function testNullCodeNonSchemaMessage(): void
    {
        $classifier = new PgSqlErrorClassifier();
        $e = new DatabaseException('Connection refused', null);
        self::assertFalse($classifier->isUnknownSchemaError($e));
    }

    public function testColumnDoesNotExistUpperCase(): void
    {
        $classifier = new PgSqlErrorClassifier();
        $e = new DatabaseException('COLUMN "foo" DOES NOT EXIST', null);
        self::assertTrue($classifier->isUnknownSchemaError($e));
    }

    public function testRelationDoesNotExistUpperCase(): void
    {
        $classifier = new PgSqlErrorClassifier();
        $e = new DatabaseException('RELATION "bar" DOES NOT EXIST', null);
        self::assertTrue($classifier->isUnknownSchemaError($e));
    }

    public function testTableDoesNotExistPattern(): void
    {
        $classifier = new PgSqlErrorClassifier();
        $e = new DatabaseException('table "baz" does not exist', null);
        self::assertTrue($classifier->isUnknownSchemaError($e));
    }

    public function testTableDoesNotExistUpperCase(): void
    {
        $classifier = new PgSqlErrorClassifier();
        $e = new DatabaseException('TABLE "baz" DOES NOT EXIST', null);
        self::assertTrue($classifier->isUnknownSchemaError($e));
    }

    public function testSqlstateOnlyWithoutPatternMatch(): void
    {
        $classifier = new PgSqlErrorClassifier();
        $e = new DatabaseException('42703', null);
        self::assertTrue($classifier->isUnknownSchemaError($e));
    }

    public function testSqlstate42P01OnlyWithoutPatternMatch(): void
    {
        $classifier = new PgSqlErrorClassifier();
        $e = new DatabaseException('42P01', null);
        self::assertTrue($classifier->isUnknownSchemaError($e));
    }

    public function testSqlstate42P02OnlyWithoutPatternMatch(): void
    {
        $classifier = new PgSqlErrorClassifier();
        $e = new DatabaseException('Error 42P02', null);
        self::assertTrue($classifier->isUnknownSchemaError($e));
    }

    public function testSqlstate42P10OnlyWithoutPatternMatch(): void
    {
        $classifier = new PgSqlErrorClassifier();
        $e = new DatabaseException('Error 42P10', null);
        self::assertTrue($classifier->isUnknownSchemaError($e));
    }

    public function testSqlstate42704OnlyWithoutPatternMatch(): void
    {
        $classifier = new PgSqlErrorClassifier();
        $e = new DatabaseException('Error 42704', null);
        self::assertTrue($classifier->isUnknownSchemaError($e));
    }

    public function testDriverCodeOnlyWithoutMessageMatch(): void
    {
        $classifier = new PgSqlErrorClassifier();
        $e = new DatabaseException('Some generic error', 7);
        self::assertTrue($classifier->isUnknownSchemaError($e));
    }

    public function testNullCodeAndNoPatternReturnsFalse(): void
    {
        $classifier = new PgSqlErrorClassifier();
        $e = new DatabaseException('Some other error', null);
        self::assertFalse($classifier->isUnknownSchemaError($e));
    }

    public function testNonSchemaDriverCodeReturnsFalse(): void
    {
        $classifier = new PgSqlErrorClassifier();
        $e = new DatabaseException('Some error without schema code', 99);
        self::assertFalse($classifier->isUnknownSchemaError($e));
    }

    public function testDriverCodeZeroIsNotSchemaError(): void
    {
        $classifier = new PgSqlErrorClassifier();
        $e = new DatabaseException('General failure', 0);
        self::assertFalse($classifier->isUnknownSchemaError($e));
    }
}
