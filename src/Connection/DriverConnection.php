<?php

declare(strict_types=1);

namespace Qubus\Expressive\Connection;

use Qubus\Exception\Data\TypeException;
use Qubus\Expressive\Connection;
use Qubus\Expressive\ParsePdoDsn;
use Throwable;

use function sprintf;
use function str_starts_with;

class DriverConnection
{
    /**
     * Build a driver-native connection (extension-specific).
     *
     * @throws TypeException
     */
    public static function make(array|string $config): Connection
    {
        if (is_string($config)) {
            $config = self::parseDsn($config);
        }

        $driver = $config['driver'] ?? null;
        if (! is_string($driver) || $driver === '') {
            throw new TypeException(message: 'A PDO driver must be provided.');
        }

        $config['driver'] = str_starts_with($driver, 'pdo_') ? $driver : sprintf('pdo_%s', $driver);

        return match ($config['driver']) {
            'pdo_mysql'  => new Connection\Pdo\Mysql($config),
            'pdo_pgsql'  => new Connection\Pdo\Pgsql($config),
            'pdo_sqlite' => new Connection\Pdo\Sqlite($config),
            'pdo_oci'    => new Connection\Pdo\Oci($config),
            'pdo_sqlsrv' => new Connection\Pdo\Sqlsrv($config),
            default => throw new TypeException(message: "Unsupported PDO driver"),
        };
    }

    /**
     * Parse a DSN-like URL into config array.
     *
     * Example: mysql://user:pass@localhost:3306/dbname?charset=utf8mb4
     *
     * @throws TypeException
     */
    protected static function parseDsn(string $url): array
    {
        try {
            $dsn = ParsePdoDsn::fromString($url);
        } catch (Throwable) {
            throw new TypeException(message: sprintf("Invalid DSN: %s", $url));
        }

        $config = $dsn->toArray();
        if (isset($config['user'])) {
            $config['username'] = $config['user'];
            unset($config['user']);
        }

        return $config;
    }
}
