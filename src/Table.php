<?php

declare(strict_types=1);

namespace Qubus\Expressive;

interface Table
{
    /**
     * Define the working table and create a new instance
     *
     * @param string $tableName Table name.
     * @param ?string $alias     The table alias name.
     * @return Database
     */
    public function table(string $tableName, ?string $alias = null): Database;

    /**
     * Return the name of the table.
     */
    public function getTableName(): string;

    /**
     * Set the table alias.
     *
     * @param string $alias
     * @return Database
     */
    public function setTableAlias(string $alias): Database;

    /**
     * Get table Alias
     */
    public function getTableAlias(): string;

    /**
     * @param string $primaryKeyName The primary key, ie: id
     * @param string $foreignKeyName The foreign key as a pattern: %s_id,
     *                               where %s will be substituted with the table name
     * @return Database
     */
    public function setStructure(string $primaryKeyName = 'id', string $foreignKeyName = '%s_id'): Database;

    /**
     * @param string|null $tablePrefix
     * @return Database
     */
    public function setTablePrefix(?string $tablePrefix = ''): Database;

    /**
     * Return the table prefix.
     */
    public function getTablePrefix(): ?string;

    /**
     * Return the table structure.
     *
     * @return array
     */
    public function getStructure(): array;

    /**
     * Get the primary key name.
     *
     * @return string
     */
    public function getPrimaryKeyname(): string;

    /**
     * Get foreign key name.
     *
     * @return string
     */
    public function getForeignKeyname(): string;
}
