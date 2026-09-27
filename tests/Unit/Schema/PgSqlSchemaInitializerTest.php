<?php

declare(strict_types=1);

namespace Tests\Unit\Schema;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Schema\PgSqlSchemaInitializer::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Relation\FromClause::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Relation\RelationReference::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Schema\PgSqlColumnTypeMapper::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Schema\Key\PgSqlForeignKeyDefinitionParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlIdentifierQuoter::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Partition\PgSqlPartitionParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Schema\Partition\PgSqlPartitionReflector::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Schema\PgSqlSchemaParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Schema\PgSqlSchemaReflector::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Relation\PgSqlSelectRelationParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Schema\View\PgSqlViewDefinitionParser::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Schema\Reflection\Catalog\PartitionKeys::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Schema\Reflection\Catalog\TableQueries::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Schema\Reflection\Column\ColumnDefinitionSql::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Schema\Reflection\Column\NativeTypeSql::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Schema\Reflection\Key\ForeignKeyRow::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Schema\Reflection\Key\ForeignKeys::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Schema\Reflection\Key\IndexDefinitions::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Schema\Reflection\Key\IndexRows::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Schema\Reflection\Key\PrimaryColumns::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Schema\Reflection\Key\UniqueIndexes::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Schema\Definition\ColumnDefinition::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Schema\Definition\ColumnTypeDeclaration::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Schema\Definition\TableBody::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Schema\Definition\TableConstraint::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Schema\Definition\TableFields::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Schema\Key\DefinitionEntry::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Schema\Key\DefinitionTokens::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Partition\BoundPredicate::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Partition\ClauseTokens::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Sql\Lexing\QuotedSpan::class)]
final class PgSqlSchemaInitializerTest extends TestCase
{
    public function testPopulateRegistersColumnsPartialIndexesAndPartitionKeys(): void
    {
        $statement = self::createStub(\ZtdQuery\Connection\StatementInterface::class);
        $statement->method('fetchAll')->willReturn([['table_name' => 'users']]);
        $statement2 = self::createStub(\ZtdQuery\Connection\StatementInterface::class);
        $statement2->method('fetchAll')->willReturn([['column_name' => 'id', 'data_type' => 'integer', 'is_nullable' => 'NO']]);
        $statement3 = self::createStub(\ZtdQuery\Connection\StatementInterface::class);
        $statement3->method('fetchAll')->willReturn([['column_name' => 'id']]);
        $statement4 = self::createStub(\ZtdQuery\Connection\StatementInterface::class);
        $statement4->method('fetchAll')->willReturn([['constraint_name' => 'positive_id', 'column_name' => 'id', 'predicate' => 'id > 0']]);
        $statement5 = self::createStub(\ZtdQuery\Connection\StatementInterface::class);
        $statement5->method('fetchAll')->willReturn([]);
        $statement6 = self::createStub(\ZtdQuery\Connection\StatementInterface::class);
        $statement6->method('fetchAll')->willReturn([['table_name' => 'users', 'partition_key' => 'RANGE (id)']]);
        $statement7 = self::createStub(\ZtdQuery\Connection\StatementInterface::class);
        $statement7->method('fetchAll')->willReturn([]);
        $statementResults = [$statement, $statement2, $statement3, $statement4, $statement5, $statement6, $statement7];
        $connection = self::createStub(\ZtdQuery\Connection\ConnectionInterface::class);
        $connection->method('query')->willReturnCallback(static function () use (&$statementResults): \ZtdQuery\Connection\StatementInterface|false {
            return array_shift($statementResults) ?? false;
        });
        $registry = new \ZtdQuery\Schema\TableDefinitionRegistry();
        (new \ZtdQuery\Platform\Postgres\Schema\PgSqlSchemaInitializer())->populate($connection, new \ZtdQuery\Platform\Postgres\Schema\PgSqlSchemaReflector($connection), new \ZtdQuery\Platform\Postgres\Schema\PgSqlSchemaParser(), $registry);
        $definition = $registry->get('users');
        self::assertNotNull($definition);
        self::assertSame(['id'], $definition->primaryKeys);
        self::assertSame('id > 0', $definition->partialUniqueIndexes['positive_id']->predicate);
        self::assertSame(['id'], $definition->partitionKey?->expressions);
    }
}
