<?php

declare(strict_types=1);

namespace Qubus\Expressive;

use ArrayIterator;
use InternalIterator;
use PDOStatement;
use SplFixedArray;

interface Database
{
    /**
     * Define the working table and create a new instance
     *
     * @param string $tableName Table name.
     * @param ?string $alias     The table alias name.
     */
    public function table(string $tableName, ?string $alias = null): self;

    /**
     * @param string $primaryKeyName The primary key, ie: id
     * @param string $foreignKeyName The foreign key as a pattern: %s_id,
     *                               where %s will be substituted with the table name
     * @return QueryBuilder
     */
    public function setStructure(string $primaryKeyName = 'id', string $foreignKeyName = '%s_id'): self;

    /**
     * To execute a raw query
     *
     * @param string $query
     * @param array $parameters
     * @param bool $returnAsPdoStmt       True, it will return the PDOStatement
     *                                    false, it will return $this, which can be used for chaining
     *                                    or access the properties of the results.
     * @return QueryBuilder|PDOStatement
     */
    public function query(string $query, array $parameters = [], bool $returnAsPdoStmt = false): static|PDOStatement;

    /**
     * To find all rows and create their instances
     * Use the query builder to build the where clause or $this->query with select
     * If a callback function is provided, the 1st arg must accept the rows results
     *
     * $this->find(function($rows){
     *   // do more stuff here...
     * });
     *
     * @param callable|null $callback Run a function on the returned rows
     * @return bool|SplFixedArray|string|ArrayIterator|InternalIterator|array
     */
    public function find(?callable $callback = null): mixed;

    /**
     * Return one row
     *
     * @param int|string|null $id Use to fetch by primary key.
     * @return QueryBuilder|false
     */
    public function findOne(int|string|null $id = null): self|bool;

    /**
     * Create the select clause.
     *
     * @param mixed $columns The column(s) to select. Can be string or array of fields.
     * @param string|null $alias An alias to the column.
     * @return QueryBuilder
     */
    public function select(mixed $columns = '*', ?string $alias = null): self;

    /**
     * Add where condition, more calls appends with AND.
     *
     * @param mixed $condition condition possibly containing ? or :name
     * @param mixed $parameters array accepted by PDOStatement::execute or a scalar value.
     * @return QueryBuilder
     */
    public function where(mixed $condition, mixed $parameters = null): self;
}
