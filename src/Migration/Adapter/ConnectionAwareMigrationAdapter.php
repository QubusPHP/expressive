<?php

declare(strict_types=1);

namespace Qubus\Expressive\Migration\Adapter;

use Qubus\Expressive\Connection;

interface ConnectionAwareMigrationAdapter
{
    public function connection(): Connection;
}
