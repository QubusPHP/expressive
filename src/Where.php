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
     * @return self
     */
    public function where(mixed $condition, mixed $parameters = null): self;

    /**
     * Where Primary key
     *
     * @param int|string $id
     * @return self
     */
    public function wherePK(int|string $id): self;

    /**
     * WHERE $columName != $value
     *
     * @param string $columnName
     * @param mixed $value
     * @return self
     */
    public function whereNot(string $columnName, mixed $value): self;

    /**
     * WHERE $columName LIKE $value
     *
     * @param string $columnName
     * @param mixed $value
     * @return self
     */
    public function whereLike(string $columnName, mixed $value): self;

    /**
     * WHERE $columName NOT LIKE $value
     *
     * @param string $columnName
     * @param mixed $value
     * @return self
     */
    public function whereNotLike(string $columnName, mixed $value): self;

    /**
     * WHERE $columName > $value
     *
     * @param string $columnName
     * @param mixed $value
     * @return self
     */
    public function whereGt(string $columnName, mixed $value): self;

    /**
     * WHERE $columName >= $value
     *
     * @param string $columnName
     * @param mixed $value
     * @return self
     */
    public function whereGte(string $columnName, mixed $value): self;

    /**
     * WHERE $columName < $value
     *
     * @param string $columnName
     * @param mixed $value
     * @return self
     */
    public function whereLt(string $columnName, mixed $value): self;

    /**
     * WHERE $columName <= $value
     *
     * @param string $columnName
     * @param mixed $value
     * @return self
     */
    public function whereLte(string $columnName, mixed $value): self;

    /**
     * WHERE $columName IN (?,?,?,...)
     *
     * @param string $columnName
     * @param array $values
     * @return self
     */
    public function whereIn(string $columnName, array $values): self;

    /**
     * WHERE $columName NOT IN (?,?,?,...)
     *
     * @param string $columnName
     * @param array $values
     * @return self
     */
    public function whereNotIn(string $columnName, array $values): self;

    /**
     * WHERE $columName IS NULL
     *
     * @param string $columnName
     * @return self
     */
    public function whereNull(string $columnName): self;

    /**
     * WHERE $columName IS NOT NULL
     *
     * @param string $columnName
     * @return self
     */
    public function whereNotNull(string $columnName): self;

    /**
     * ORDER BY $columnName (ASC | DESC)
     *
     * @param  string   $columnName - The name of the colum or an expression
     * @param  string   $ordering   (DESC | ASC)
     * @return self
     */
    public function orderBy(string $columnName, string $ordering = ''): self;

    /**
     * LIMIT $limit
     *
     * @param int|null $limit
     * @return self|int|null
     */
    public function limit(?int $limit = null): self|int|null;

    /**
     * OFFSET $offset
     *
     * @param int|null $offset
     * @return self|int|null
     */
    public function offset(?int $offset = null): self|int|null;

    /**
     * @param int $perPage
     * @param int $page
     *
     * @return self
     */
    public function pagination(int $perPage, int $page): self;
}
