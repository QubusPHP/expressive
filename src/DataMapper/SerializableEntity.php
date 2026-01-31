<?php

declare(strict_types=1);

namespace Qubus\Expressive\DataMapper;

use Stringable;

class SerializableEntity implements Stringable
{
    public function __toString()
    {
        return json_encode($this);
    }
}
