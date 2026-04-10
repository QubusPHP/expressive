<?php

declare(strict_types=1);

namespace Qubus\Expressive;

use Closure;
use Exception;

interface Database extends Singleton, Table, Select, Where, Insert, Update, Set, Delete, Join, Aggregate
{
    /**
     * Whether to return as object.
     */
    public const string OBJECT = 'OBJECT';

    /**
     * Whether to return as an associative array.
     */
    public const string ARRAY_A = 'ARRAY_A';

    /**
     * Whether to return as a numeric array.
     */
    public const string ARRAY_N = 'ARRAY_N';

    /**
     * Whether to return as a JSON object.
     */
    public const string JSON_OBJECT = 'JSON_OBJECT';

    public function getConnection(): Connection;

    /**
     * The associated schema instance.
     */
    public function schema(): Schema;

    /**
     * Run transactional queries.
     *
     * @param Closure $callback transaction callback
     * @throws Exception
     */
    public function transactional(Closure $callback, mixed $that = null, mixed $default = null): mixed;

    /**
     * Prepares positional or named placeholders in a query string.
     *
     * @param string $query
     * @param mixed ...$params
     * @return string
     */
    public function prepare(string $query, mixed ...$params): string;

    /**
     * Wrapper for PDO::quote.
     *
     * @param string|array $value
     * @return false|string Quoted string.
     */
    public function quote(string|array $value): string|false;

    /**
     * @param array<int|string, scalar|null> $params
     * @return list<array<string, mixed>>
     */
    public function raw(string $sql, array $params = []): array;

    /**
     * Retrieve an entire SQL result set from the database (i.e. many rows).
     *
     * @param string|null $query
     * @param string $output
     * @return false|string|array
     */
    public function getResults(?string $query = null, string $output = self::OBJECT): false|string|array;

    /**
     * Retrieve one variable from the database.
     *
     * @param string|null $query
     * @param int $x
     * @param int $y
     * @return string|int|null
     */
    public function getVar(?string $query = null, int $x = 0, int $y = 0): string|int|null;

    /**
     * Retrieve one column from the database.
     *
     * @param string|null $query
     * @param int $x
     * @return array|null
     */
    public function getCol(?string $query = null, int $x = 0): ?array;

    /**
     * Retrieve one row from the database.
     *
     * @param string|null $query
     * @param string $output
     * @param int $y
     * @return object|array|null
     */
    public function getRow(?string $query = null, string $output = self::OBJECT, int $y = 0): object|array|null;
}
