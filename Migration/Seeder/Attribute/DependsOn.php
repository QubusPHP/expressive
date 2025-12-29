<?php

declare(strict_types=1);

namespace Qubus\Expressive\Migration\Seeder\Attribute;

use Attribute;

#[Attribute(Attribute::TARGET_CLASS | Attribute::IS_REPEATABLE)]
final readonly class DependsOn
{
    public function __construct(public string $seeder)
    {
    }
}
