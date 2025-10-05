<?php

declare(strict_types=1);

namespace Qubus\Expressive;

class Identifier extends Expression
{
    /**
     * Handles identifier quoting.
     *
     * @return Connection $connection quoted identifier
     */
    public function handle(mixed $connection): mixed
    {
        return $connection->quoteIdentifier($this->value);
    }
}
