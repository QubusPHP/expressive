<?php

declare(strict_types=1);

namespace Qubus\Expressive\Traits;

use function array_map;
use function implode;
use function is_array;
use function is_bool;
use function is_float;
use function is_int;
use function sprintf;
use function str_replace;

trait IdentifierAware
{
    public function quoteIdentifier(string $identifier): string
    {
        return match ($this->driverName) {
            'mysql'  => '`' . str_replace('`', '``', $identifier) . '`',
            'pgsql', 'sqlite', 'oci' => '"' . str_replace('"', '""', $identifier) . '"',
            'sqlsrv' => '[' . str_replace(']', ']]', $identifier) . ']',
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
     * @param float|array<array-key, float|bool|int|string|null>|bool|int|string|null $value any value to quote
     * @return false|int|string
     */
    public function quote(float|array|bool|int|string|null $value = null): false|int|string
    {
        if ($value === null) {
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
            return $value;
        }

        if (is_float(value: $value)) {
            // Convert to non-locale aware float to prevent possible commas.
            return sprintf('%F', $value);
        }

        return $this->pdo->quote(string: $value);
    }
}
