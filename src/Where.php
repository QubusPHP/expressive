<?php

declare(strict_types=1);

namespace Qubus\Expressive;

interface Where
{
    /**
     * Add where condition, more calls appends with AND.
     *
     * @param mixed $condition condition possibly containing ? or :name
     * @param mixed $parameters array accepted by PDOStatement::execute or a scalar value.
     * @return Database
     */
    public function where(mixed $condition, mixed $parameters = null): Database;

    /**
     * Where Primary key
     *
     * @param int|string $id
     * @return Database
     */
    public function wherePK(int|string $id): Database;

    /**
     * WHERE $columName != $value
     *
     * @param string $columnName
     * @param mixed $value
     * @return Database
     */
    public function whereNot(string $columnName, mixed $value): Database;

    /**
     * WHERE $columName LIKE $value
     *
     * @param string $columnName
     * @param mixed $value
     * @return Database
     */
    public function whereLike(string $columnName, mixed $value): Database;

    /**
     * WHERE $columName NOT LIKE $value
     *
     * @param string $columnName
     * @param mixed $value
     * @return Database
     */
    public function whereNotLike(string $columnName, mixed $value): Database;

    /**
     * WHERE $columName > $value
     *
     * @param string $columnName
     * @param mixed $value
     * @return Database
     */
    public function whereGt(string $columnName, mixed $value): Database;

    /**
     * WHERE $columName >= $value
     *
     * @param string $columnName
     * @param mixed $value
     * @return Database
     */
    public function whereGte(string $columnName, mixed $value): Database;

    /**
     * WHERE $columName < $value
     *
     * @param string $columnName
     * @param mixed $value
     * @return Database
     */
    public function whereLt(string $columnName, mixed $value): Database;

    /**
     * WHERE $columName <= $value
     *
     * @param string $columnName
     * @param mixed $value
     * @return Database
     */
    public function whereLte(string $columnName, mixed $value): Database;

    /**
     * WHERE $columName IN (?,?,?,...)
     *
     * @param string $columnName
     * @param array $values
     * @return Database
     */
    public function whereIn(string $columnName, array $values): Database;

    /**
     * WHERE $columName NOT IN (?,?,?,...)
     *
     * @param string $columnName
     * @param array $values
     * @return Database
     */
    public function whereNotIn(string $columnName, array $values): Database;

    /**
     * WHERE $columName IS NULL
     *
     * @param string $columnName
     * @return Database
     */
    public function whereNull(string $columnName): Database;

    /**
     * WHERE $columName IS NOT NULL
     *
     * @param string $columnName
     * @return Database
     */
    public function whereNotNull(string $columnName): Database;

    /**
     * ORDER BY $columnName (ASC | DESC)
     *
     * @param  string   $columnName - The name of the colum or an expression
     * @param  string   $ordering   (DESC | ASC)
     * @return Database
     */
    public function orderBy(string $columnName, string $ordering = 'ASC'): Database;

    /**
     * LIMIT $limit
     *
     * @param int|null $limit
     * @return Database|int|null
     */
    public function limit(?int $limit = null): Database|int|null;

    /**
     * OFFSET $offset
     *
     * @param int|null $offset
     * @return Database|int|null
     */
    public function offset(?int $offset = null): Database|int|null;

    /**
     * @param int $perPage
     * @param int $page
     * @return Database
     */
    public function pagination(int $perPage, int $page): Database;
}
