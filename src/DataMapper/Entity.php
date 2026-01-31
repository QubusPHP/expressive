<?php

declare(strict_types=1);

namespace Qubus\Expressive\DataMapper;

use Attribute;

#[Attribute(Attribute::TARGET_CLASS)]
class Entity
{
    private ?string $table = null;

    public function __construct(?string $table = null)
    {
        $this->table = $table;
    }

    public function getTable(): string
    {
        return $this->table;
    }

    public function setTable(string $table): void
    {
        $this->table = $table;
    }
}
