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
     * @return MigrationAdapter
     */
    public function up(Migration $migration): MigrationAdapter;

    /**
     * Down
     *
     * @param Migration $migration
     * @return MigrationAdapter
     */
    public function down(Migration $migration): MigrationAdapter;

    /**
     * Is the schema ready?
     *
     * @return bool
     */
    public function hasSchema(): bool;

    /**
     * Create Schema
     *
     * @return MigrationAdapter
     */
    public function createSchema(): MigrationAdapter;
}
