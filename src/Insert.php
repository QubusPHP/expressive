<?php

declare(strict_types=1);

namespace Qubus\Expressive;

interface Insert
{
    /**
     * Returning (Postgres etc.)
     *
     * @param string $cols
     * @return Database
     */
    public function returning(string $cols = '*'): Database;

    /**
     * Upsert (basic support)
     * @param list<string> $conflictCols
     * @param array<string, mixed> $updateData
     * @return Database
     */
    public function upsert(array $conflictCols, array $updateData): Database;

    /**
     * Retrieves the ID of the last record inserted.
     *
     * @param string|null $pk
     * @return string|false
     */
    public function lastInsertId(string|null $pk = null): string|false;

    /**
     * Insert one or more rows. Bulk rows must contain identical columns in identical order.
     * If a single row is inserted, its row instance is returned. Bulk inserts return the affected row count.
     *
     * @param array<string, mixed>|list<array<string, mixed>> $data Data to insert.
     * @return Database|int
     * @throws QueryBuilderException When the payload is empty or bulk rows have inconsistent columns.
     */
    public function insert(array $data): Database|int;
}
