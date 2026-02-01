<?php

declare(strict_types=1);

namespace Qubus\Expressive;

use ArrayIterator;
use InternalIterator;
use Iterator;
use PDOStatement;
use SplFixedArray;

interface Select
{
    public const string OPERATOR_AND = ' AND ';
    public const string OPERATOR_OR = ' OR ';

    /**
     * To execute a raw query
     *
     * @param string $query
     * @param array $parameters
     * @param bool $returnAsPdoStmt       True, it will return the PDOStatement
     *                                    false, it will return $this, which can be used for chaining
     *                                    or access the properties of the results.
     * @return Database|PDOStatement
     */
    public function query(string $query, array $parameters = [], bool $returnAsPdoStmt = false): Database|PDOStatement;

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
     * @return callable|bool|int|SplFixedArray|string|ArrayIterator|InternalIterator|array|Iterator
     */
    public function find(?callable $callback = null): mixed;

    /**
     * Return one row
     *
     * @param int|string|null $id Use to fetch by primary key.
     * @return Database|false
     */
    public function findOne(int|string|null $id = null): Database|bool;

    /**
     * Create the select clause.
     *
     * @param mixed $columns The column(s) to select. Can be string or array of fields.
     * @param string|null $alias An alias to the column.
     * @return Database
     */
    public function select(mixed $columns = '*', ?string $alias = null): Database;

    /**
     * Return the select fields as array.
     *
     * @return array
     */
    public function getSelectFields(): array;

    /**
     * Create an instance from the given row (an associative
     * array of data fetched from the database).
     *
     * @param array $data
     * @return Database
     */
    public function fromArray(array $data): Database;

    /**
     * @param $statement
     * @param string $operator
     * @return Database
     */
    public function having($statement, string $operator = self::OPERATOR_AND): Database;

    /**
     * GROUP BY $columnName
     *
     * @param string $columnName
     * @return Database
     */
    public function groupBy(string $columnName): Database;
}
