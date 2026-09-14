<?php

declare(strict_types=1);

namespace Tests\Unit\Schema\View;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Platform\Postgres\Schema\View\PgSqlViewDefinitionParser;
use ZtdQuery\Platform\Postgres\Sql\Relation\PgSqlSelectRelationParser;

#[CoversClass(PgSqlViewDefinitionParser::class)]
#[UsesClass(PgSqlSelectRelationParser::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\PgSqlLexerProfile::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Relation\FromClause::class)]
#[UsesClass(\ZtdQuery\Platform\Postgres\Sql\Relation\RelationReference::class)]
final class PgSqlViewDefinitionParserTest extends TestCase
{
    public function testFromQueryParsesAQueryUsingPostgreSqlRelationRules(): void
    {
        $definition = (new PgSqlViewDefinitionParser())->fromQuery(
            " SELECT u.id FROM public.\"users\" u JOIN roles r ON r.id = u.role_id; \n",
        );

        self::assertSame('SELECT u.id FROM public."users" u JOIN roles r ON r.id = u.role_id', $definition->query);
        self::assertSame(['users', 'roles'], $definition->dependencies);
    }

    public function testFromCreateStatementExtractsTheQueryFromAPostgreSqlCreateViewStatement(): void
    {
        $definition = (new PgSqlViewDefinitionParser())->fromCreateStatement(
            'CREATE OR REPLACE VIEW "active_users" AS SELECT * FROM "users" WHERE active = TRUE;',
        );

        self::assertNotNull($definition);
        self::assertSame('SELECT * FROM "users" WHERE active = TRUE', $definition->query);
        self::assertNull((new PgSqlViewDefinitionParser())->fromCreateStatement('CREATE VIEW invalid AS   '));
    }
}
