<?php

declare(strict_types=1);

namespace Qubus\Expressive;

use ArrayIterator;
use Closure;
use Exception;
use InternalIterator;
use Opis\Database\Schema;
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
    public function table(string $tableName, ?string $alias = null): static;

    /**
     * @param string $primaryKeyName The primary key, ie: id
     * @param string $foreignKeyName The foreign key as a pattern: %s_id,
     *                               where %s will be substituted with the table name
     * @return QueryBuilder
     */
    public function setStructure(string $primaryKeyName = 'id', string $foreignKeyName = '%s_id'): static;

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
    public function findOne(int|string|null $id = null): static|bool;

    /**
     * Create the select clause.
     *
     * @param mixed $columns The column(s) to select. Can be string or array of fields.
     * @param string|null $alias An alias to the column.
     * @return QueryBuilder
     */
    public function select(mixed $columns = '*', ?string $alias = null): static;

    /**
     * Add where condition, more calls appends with AND.
     *
     * @param mixed $condition condition possibly containing ? or :name
     * @param mixed $parameters array accepted by PDOStatement::execute or a scalar value.
     * @return QueryBuilder
     */
    public function where(mixed $condition, mixed $parameters = null): static;

    /**
     * The associated schema instance.
     */
    public function schema(): Schema;

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
     * @return QueryBuilder|int
     */
    public function insert(array $data): static|int;

    /**
     * Update entries
     * Use the query builder to create the where clause.
     *
     * @param array|null $data the data to update
     * @return QueryBuilder|int|false
     */
    public function update(?array $data = null): static|int|false;

    /**
     * Delete rows.
     *
     * Use the query builder to create the where clause.
     *
     * @param bool $deleteAll When there is no where condition, setting to true will delete all.
     * @return QueryBuilder|int|false
     */
    public function delete(bool $deleteAll = false): static|int|false;

    /**
     * Run transactional queries.
     *
     * @param Closure $callback transaction callback
     * @throws Exception
     */
    public function transactional(Closure $callback, mixed $that = null, mixed $default = null): mixed;
}
