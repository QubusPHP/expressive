<?php

declare(strict_types=1);

namespace Qubus\Expressive;

use PDO;

interface Connection
{
    public PDO $pdo { get; }

    public function query(string $sql, array $params = []): ResultSet;

    public function queryBuilder(): QueryBuilder;

    public function getSchema(): Schema;

    /**
     * Driver feature detection.
     *
     * @return bool
     */
    public function supportsReturning(): bool;

    /**
     * @return bool
     */
    public function supportsUpsert(): bool;
}
