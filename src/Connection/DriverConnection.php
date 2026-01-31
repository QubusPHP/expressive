<?php

declare(strict_types=1);

namespace Qubus\Expressive\Connection;

use Qubus\Exception\Data\TypeException;
use Qubus\Expressive\Connection;

use function sprintf;
use function str_contains;

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

        if (is_array($config)) {
            $config['driver'] = !str_contains($config['driver'], 'pdo_')
            ? sprintf('pdo_%s', $config['driver'])
            : $config['driver'];
        }

        return match ($config['driver'] ?? null) {
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
        $parts = parse_url($url);
        if ($parts === false) {
            throw new TypeException(message: sprintf("Invalid DSN: %s", $url));
        }

        $scheme = $parts['scheme'] ?? '';
        $driver = "pdo_{$scheme}";

        $query = [];
        if (!empty($parts['query'])) {
            parse_str($parts['query'], $query);
        }

        return [
            'driver'   => $driver,
            'host'     => $parts['host'] ?? null,
            'port'     => $parts['port'] ?? null,
            'username'     => $parts['user'] ?? null,
            'password'     => $parts['pass'] ?? null,
            'dbname'   => ltrim(string: $parts['path'] ?? '', characters: '/'),
            ...$query
        ];
    }
}
