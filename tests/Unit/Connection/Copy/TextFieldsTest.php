<?php

declare(strict_types=1);

namespace Tests\Unit\Connection\Copy;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(\ZtdQuery\Platform\Postgres\Connection\Copy\TextFields::class)]
final class TextFieldsTest extends TestCase
{
    public function testEscape(): void
    {
        self::assertSame('a\\tb\\nc\\\\d', (new \ZtdQuery\Platform\Postgres\Connection\Copy\TextFields())->escape('a	b
c\\d', '	'));
    }

    public function testDecodeFields(): void
    {
        self::assertSame(['1', null, 'line
next'], (new \ZtdQuery\Platform\Postgres\Connection\Copy\TextFields())->decodeFields('1	\\N	line\\nnext', '	', '\\N'));

        self::assertSame(['A', 'B', '	'], (new \ZtdQuery\Platform\Postgres\Connection\Copy\TextFields())->decodeFields('\\101	\\x42	\\t', '	', '\\N'));

        self::assertSame(['a	b', ''], (new \ZtdQuery\Platform\Postgres\Connection\Copy\TextFields())->decodeFields('a\\	b	', '	', '\\N'));
    }

    public function testValidateSeparator(): void
    {
        (new \ZtdQuery\Platform\Postgres\Connection\Copy\TextFields())->validateSeparator('	');

        self::assertSame('unchanged', (new \ZtdQuery\Platform\Postgres\Connection\Copy\TextFields())->escape('unchanged', "\t"));
    }
    public function testDecodeEscapeAdvancesOverOneEncodedByte(): void
    {
        $index = 0;
        self::assertSame("\xFF", (new \ZtdQuery\Platform\Postgres\Connection\Copy\TextFields())->decodeEscape('\\377', $index));
        self::assertSame(3, $index);
    }

    public function testOctalByteConsumesAtMostThreeDigits(): void
    {
        $index = 0;
        self::assertSame("\xFF", (new \ZtdQuery\Platform\Postgres\Connection\Copy\TextFields())->octalByte('377rest', $index, '3'));
        self::assertSame(2, $index);
    }

    public function testHexByteConsumesTwoDigitsAfterThePrefix(): void
    {
        $index = 0;
        self::assertSame('A', (new \ZtdQuery\Platform\Postgres\Connection\Copy\TextFields())->hexByte('x41rest', $index, '4'));
        self::assertSame(2, $index);
    }
}
