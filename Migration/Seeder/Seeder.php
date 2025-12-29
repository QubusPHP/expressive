<?php

declare(strict_types=1);

namespace Qubus\Expressive\Migration\Seeder;

interface Seeder
{
    public function run(SeederContext $context): void;
}
