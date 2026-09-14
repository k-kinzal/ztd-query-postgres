<?php

declare(strict_types=1);

namespace Tests\Unit\Schema\Definition;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Schema\Definition\TableFields::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Schema\PgSqlColumnTypeMapper::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Partition\PgSqlPartitionParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Schema\Definition\ColumnDefinition::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Schema\Definition\ColumnTypeDeclaration::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Schema\Definition\TableConstraint::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Partition\BoundPredicate::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Partition\ClauseTokens::class)]
final class TableFieldsTest extends TestCase
{
    public function testAppendEntryPreservesInlineAndTableConstraints(): void
    {
        $fields = new \ZtdQuery\Platform\Postgres\Schema\Definition\TableFields();
        $fields->appendEntry('id SERIAL PRIMARY KEY');
        $fields->appendEntry("name TEXT UNIQUE DEFAULT 'guest'");
        $fields->appendEntry('CONSTRAINT unique_pair UNIQUE (id, name)');
        $definition = $fields->definition('CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT)', []);
        self::assertNotNull($definition);
        self::assertSame(['id', 'name'], $definition->columns);
        self::assertSame(['id'], $definition->primaryKeys);
        self::assertSame(['id'], $definition->notNullColumns);
        self::assertSame("'guest'", $definition->columnDefaults['name']);
        self::assertSame(\ZtdQuery\Schema\Key\IdentityGenerationStrategy::Sequence, $definition->identityStrategies['id']);
        self::assertSame(['id', 'name'], $definition->uniqueConstraints['unique_pair']);
    }

    public function testAppendColumnRecordsGeneratedExpressions(): void
    {
        $fields = new \ZtdQuery\Platform\Postgres\Schema\Definition\TableFields();
        $column = (new \ZtdQuery\Platform\Postgres\Schema\Definition\ColumnDefinition())->parseColumnDefinition('total NUMERIC GENERATED ALWAYS AS (price * quantity) STORED');
        self::assertNotNull($column);
        $fields->appendColumn($column);
        $definition = $fields->definition('CREATE TABLE totals (total NUMERIC)', []);
        self::assertNotNull($definition);
        self::assertSame('(price * quantity)', $definition->generatedExpressions['total']);
        self::assertSame('NUMERIC', $definition->columnTypes['total']);
    }

    public function testDefinitionRejectsEmptyTablesAndUnknownUniqueColumns(): void
    {
        $fields = new \ZtdQuery\Platform\Postgres\Schema\Definition\TableFields();
        self::assertNull($fields->definition('CREATE TABLE empty ()', []));
        $fields->appendEntry('id INT');
        $fields->appendEntry('UNIQUE (missing)');
        self::assertNull($fields->definition('CREATE TABLE broken (id INT)', []));
    }
}
