<?php

declare(strict_types=1);

namespace Qubus\Expressive;

interface Singleton
{
    /**
     * @param Connection $connection
     * @param string $primaryKeyName
     * @param string|null $tablePrefix
     * @return Database
     */
    public static function fromInstance(
        Connection $connection,
        string $primaryKeyName = 'id',
        ?string $tablePrefix = null
    ): Database;
}
