<?php

declare(strict_types=1);

namespace Qubus\Expressive\DataMapper;

use Stringable;

use const JSON_THROW_ON_ERROR;

class SerializableEntity implements Stringable
{
    /**
     * @return string
     * @throws \JsonException
     */
    public function __toString(): string
    {
        return json_encode($this, JSON_THROW_ON_ERROR);
    }
}
