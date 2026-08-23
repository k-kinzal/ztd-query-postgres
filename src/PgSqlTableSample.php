<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres;

final class PgSqlTableSample
{
    public function __construct(
        public readonly string $tableName,
        public readonly string $sourceSql,
        public readonly string $aliasSql,
        public readonly PgSqlTableSampleMethod $method,
        public readonly string $percentageSql,
        public readonly ?string $seedSql,
        public readonly int $startOffset,
        public readonly int $endOffset,
    ) {
        if ($tableName === '' || $sourceSql === '' || $percentageSql === '') {
            throw new \InvalidArgumentException('TABLESAMPLE fields must not be empty');
        }
        if ($startOffset < 0 || $endOffset <= $startOffset) {
            throw new \InvalidArgumentException('TABLESAMPLE offsets are invalid');
        }
    }
}
