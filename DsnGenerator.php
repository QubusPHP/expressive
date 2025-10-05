<?php

declare(strict_types=1);

namespace Qubus\Expressive;

use Qubus\Expressive\Connection\Pdo\Mysql;
use Qubus\Expressive\Connection\Pdo\Oci;
use Qubus\Expressive\Connection\Pdo\Pgsql;
use Qubus\Expressive\Connection\Pdo\Sqlite;
use Qubus\Expressive\Connection\Pdo\Sqlsrv;

class DsnGenerator
{
    public static function mysql(array $config): string
    {
        return Mysql::buildDsn($config);
    }

    public static function pgsql(array $config): string
    {
        return Pgsql::buildDsn($config);
    }

    public static function sqlite(array $config): string
    {
        return Sqlite::buildDsn($config);
    }

    public static function oci(array $config): string
    {
        return Oci::buildDsn($config);
    }

    public static function sqlsrv(array $config): string
    {
        return Sqlsrv::buildDsn($config);
    }
}
