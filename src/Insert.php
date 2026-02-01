<?php

declare(strict_types=1);

namespace Qubus\Expressive;

interface Insert
{
    /**
     * Returning (Postgres etc.)
     *
     * @param string $cols
     * @return self
     */
    public function returning(string $cols = '*'): self;

    /**
     * Upsert (basic support)
     * @param array $conflictCols
     * @param array $updateData
     * @return self
     */
    public function upsert(array $conflictCols, array $updateData): self;

    /**
     * Retrieves the ID of the last record inserted.
     *
     * @param string|null $pk
     * @return string|false
     */
    public function lastInsertId(string|null $pk = null): string|false;

    /**
     * Insert new rows
     * $data can be 2-dimensional to add a bulk insert
     * If a single row is inserted, it will return its row instance
     *
     * @param  array    $data - data to populate
     * @return self|int
     */
    public function insert(array $data): self|int;
}
