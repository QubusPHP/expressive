<?php

declare(strict_types=1);

namespace Qubus\Expressive;

interface Update
{
    /**
     * Update entries.
     *
     * Use the query builder to create the where clause.
     *
     * @param array|null $data the data to update
     * @return self|int|false
     */
    public function update(?array $data = null): self|int|false;
}
