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
     * @param array<string, mixed>|null $data the data to update
     * @return Database|int|false
     */
    public function update(?array $data = null): Database|int|false;
}
