<?php

declare(strict_types=1);

namespace Qubus\Expressive;

interface Aggregate
{
    /**
     * Return the aggregate count of column
     *
     * @param string|null $column - the column name
     * @return float|int
     */
    public function count(?string $column = null): float|int;

    /**
     * Return the aggregate max count of column
     *
     * @param string $column - the column name
     * @return float|int
     */
    public function max(string $column): float|int;

    /**
     * Return the aggregate min count of column
     *
     * @param string $column - the column name
     * @return float|int
     */
    public function min(string $column): float|int;

    /**
     * Return the aggregate sum count of column
     *
     * @param string $column - the column name
     * @return float|int
     */
    public function sum(string $column): float|int;

    /**
     * Return the aggregate average count of column
     *
     * @param string $column - the column name
     * @return float|int
     */
    public function avg(string $column): float|int;

    /**
     * @param string $fn - The function to use for the aggregation
     * @return float|int
     */
    public function aggregate(string $fn): float|int;
}
