<?php

declare(strict_types=1);

namespace ZtdQuery\Platform\Postgres;

use ZtdQuery\Platform\ParameterBindingCompiler;
use ZtdQuery\Sql\SqlTokenStream;

final class PgSqlPdoParameterBindingCompiler implements ParameterBindingCompiler
{
    public function __construct(
        private readonly PgSqlPdoPlaceholderEscaper $placeholderEscaper = new PgSqlPdoPlaceholderEscaper(),
    ) {
    }

    public function compile(string $sql, ?array $params): array
    {
        $replacements = [];
        $nativePositions = [];
        foreach (SqlTokenStream::tokenize($sql, PgSqlLexerProfile::create())->tokens() as $token) {
            if (!str_starts_with($token->text, '$')) {
                continue;
            }
            $position = filter_var(
                substr($token->text, 1),
                FILTER_VALIDATE_INT,
                ['options' => ['min_range' => 1]],
            );
            if (!is_int($position)) {
                continue;
            }
            $nativePositions[$position] = $position;
            $replacements[$token->offset] = [
                'length' => strlen($token->text),
                'sql' => ':__ztd_pdo_' . $position,
            ];
        }

        if ($replacements !== []) {
            krsort($replacements);
            foreach ($replacements as $offset => $replacement) {
                $sql = substr_replace($sql, $replacement['sql'], $offset, $replacement['length']);
            }
        }
        $sql = $this->placeholderEscaper->escape($sql);
        if ($nativePositions === [] || $params === null) {
            return ['sql' => $sql, 'params' => $params];
        }

        $mapped = [];
        foreach ($nativePositions as $position) {
            if (array_key_exists($position - 1, $params)) {
                $mapped['__ztd_pdo_' . $position] = $params[$position - 1];
            }
        }

        return ['sql' => $sql, 'params' => $mapped];
    }
}
