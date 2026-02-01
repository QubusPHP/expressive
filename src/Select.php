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
     * @return self|PDOStatement
     */
    public function query(string $query, array $parameters = [], bool $returnAsPdoStmt = false): self|PDOStatement;

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
     * @return self|false
     */
    public function findOne(int|string|null $id = null): self|bool;

    /**
     * Create the select clause.
     *
     * @param mixed $columns The column(s) to select. Can be string or array of fields.
     * @param string|null $alias An alias to the column.
     * @return self
     */
    public function select(mixed $columns = '*', ?string $alias = null): self;

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
     * @return self
     */
    public function fromArray(array $data): self;

    /**
     * @param $statement
     * @param string $operator
     * @return self
     */
    public function having($statement, string $operator = self::OPERATOR_AND): self;

    /**
     * GROUP BY $columnName
     *
     * @param string $columnName
     * @return self
     */
    public function groupBy(string $columnName): self;
}
