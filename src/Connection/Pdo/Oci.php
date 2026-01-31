<?php

declare(strict_types=1);

namespace Qubus\Expressive\Connection\Pdo;

use Qubus\Expressive\Connection\PdoConnection;

class Oci extends PdoConnection
{
    public static function buildDsn(array $config): string
    {
        $host = $config['host'] ?? 'localhost';
        $port = $config['port'] ?? 1521;
        $sid  = $config['dbname'] ?? 'XE';
        return "oci:dbname={$host}:{$port}/{$sid}";
    }

    public string $driverName {
        get => 'oci';
    }
}
