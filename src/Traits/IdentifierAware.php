<?php

declare(strict_types=1);

namespace Qubus\Expressive\Traits;

use Exception;
use PDO;

use function array_map;
use function implode;
use function is_array;
use function is_bool;
use function is_float;
use function is_int;
use function is_numeric;
use function is_string;
use function Qubus\Support\Helpers\is_null__;
use function sprintf;
use function trigger_error;

trait IdentifierAware
{
    public function quoteIdentifier(string $identifier): string
    {
        return match ($this->driverName) {
            'mysql'  => "`{$identifier}`",
            'pgsql', 'sqlite', 'oci' => "\"{$identifier}\"",
            'sqlsrv' => "[{$identifier}]",
            default  => $identifier,
        };
    }

    /**
     * Quote a value for an SQL query.
     *
     * Objects passed to this function will be converted to strings.
     * Expression objects will use the value of the expression.
     * Query objects will be compiled and converted to a sub-query.
     * Fnc objects will be sent of for compiling.
     * All other objects will be converted using the `__toString` method.
     *
     * @param float|array|bool|int|string|null $value any value to quote
     * @return int|string
     */
    public function quote(float|array|bool|int|string|null $value = null): int|string
    {
        try {
            if (! $this->pdo instanceof PDO) {
                throw new Exception(message: 'No PDOInstance has been made with the connection.');
            }

            if (is_null__(var: $value)) {
                return 'NULL';
            }

            if (is_bool(value: $value)) {
                return $value ? 1 : 0;
            }

            if (is_array(value: $value)) {
                return '(' .
                implode(separator: ', ', array: array_map(callback: [$this, 'quote'], array: $value)) .
                ')';
            }

            if (is_int(value: $value)) {
                return (int) $value;
            }

            if (is_float(value: $value)) {
                // Convert to non-locale aware float to prevent possible commas
                return sprintf('%F', $value);
            }

            if (is_numeric(value: $value) && ! is_string(value: $value)) {
                return (string) $value;
            }
        } catch (Exception $a) {
            trigger_error(message: $a->getMessage());
        }

        return $this->pdo->quote(string: $value);
    }
}
