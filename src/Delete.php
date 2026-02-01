<?php

declare(strict_types=1);

namespace Qubus\Expressive;

interface Delete
{
    /**
     * Delete rows.
     *
     * Use the query builder to create the where clause.
     *
     * @param bool $deleteAll When there is no where condition, setting to true will delete all.
     * @return Database|int|false
     */
    public function delete(bool $deleteAll = false): Database|int|false;
}
