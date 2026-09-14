<?php

declare(strict_types=1);

namespace Tests\Unit\Connection\Placeholder;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Connection\Placeholder\OperandInput::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Connection\Placeholder\EscapeCursor::class)]
#[\PHPUnit\Framework\Attributes\UsesClass(\ZtdQuery\Platform\Postgres\Connection\Placeholder\TokenBoundary::class)]
final class OperandInputTest extends TestCase
{
    public function testConsumeOperatorDistinguishesPlaceholdersAndOperators(): void
    {
        $input = new \ZtdQuery\Platform\Postgres\Connection\Placeholder\OperandInput();
        $cursor = new \ZtdQuery\Platform\Postgres\Connection\Placeholder\EscapeCursor('? ?|');
        self::assertTrue($input->consumeOperator($cursor));
        self::assertSame('?', $cursor->result);
        self::assertFalse($cursor->expectsOperand);
        $cursor->preserve(1);
        self::assertTrue($input->consumeOperator($cursor));
        self::assertSame('? ??|', $cursor->result);
        self::assertTrue($cursor->expectsOperand);
        self::assertSame(4, $cursor->position);
        self::assertFalse($input->consumeOperator(new \ZtdQuery\Platform\Postgres\Connection\Placeholder\EscapeCursor('word')));
    }

    public function testConsumeOperandTracksKeywordsAndNamedParameters(): void
    {
        $input = new \ZtdQuery\Platform\Postgres\Connection\Placeholder\OperandInput();
        $cursor = new \ZtdQuery\Platform\Postgres\Connection\Placeholder\EscapeCursor('SELECT');
        self::assertTrue($input->consumeOperand($cursor));
        self::assertTrue($cursor->expectsOperand);
        self::assertSame('SELECT', $cursor->result);
        $parameter = new \ZtdQuery\Platform\Postgres\Connection\Placeholder\EscapeCursor(':select');
        $parameter->preserve(1);
        self::assertTrue($input->consumeOperand($parameter));
        self::assertFalse($parameter->expectsOperand);
        $number = new \ZtdQuery\Platform\Postgres\Connection\Placeholder\EscapeCursor('12.5');
        self::assertTrue($input->consumeOperand($number));
        self::assertSame('12.5', $number->result);
        self::assertFalse($number->expectsOperand);
    }
}
