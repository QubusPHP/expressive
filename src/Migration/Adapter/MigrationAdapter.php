<?php

declare(strict_types=1);

namespace Qubus\Expressive\Migration\Adapter;

use Qubus\Expressive\Migration\Migration;

interface MigrationAdapter
{
    /**
     * Get all migrated version numbers
     *
     * @return array
     */
    public function fetchAll(): array;

    /**
     * Up
     *
     * @param Migration $migration
     * @return self
     */
    public function up(Migration $migration): self;

    /**
     * Down
     *
     * @param Migration $migration
     * @return self
     */
    public function down(Migration $migration): self;

    /**
     * Is the schema ready?
     *
     * @return bool
     */
    public function hasSchema(): bool;

    /**
     * Create Schema
     *
     * @return self
     */
    public function createSchema(): self;
}
