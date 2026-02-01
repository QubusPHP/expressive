<?php

declare(strict_types=1);

namespace Qubus\Expressive;

interface Set
{
    /**
     * To set data for update or insert
     * $key can be an array for mass set
     *
     * @param  mixed    $key
     * @param mixed|null $value
     * @return self
     */
    public function set(mixed $key, mixed $value = null): self;

    /**
     * Save, a shortcut to update() or insert().
     *
     * @return self|int|bool
     */
    public function save(): self|int|bool;
}
