<?php

declare(strict_types=1);

namespace Qubus\Expressive\Connection\Pdo;

use PDO;
use Qubus\Expressive\Connection\PdoConnection;

class Sqlsrv extends PdoConnection
{
    /**
     * Sets the connection encoding.
     *
     * @param string $charset encoding
     */
    protected function setCharset(string $charset): void
    {
        if ($charset === 'utf8' || $charset === 'utf-8') {
            // use utf8 encoding
            $this->pdo->setAttribute(attribute: PDO::SQLSRV_ATTR_ENCODING, value: PDO::SQLSRV_ENCODING_UTF8);
        } elseif ($charset === 'system') {
            // use system encoding
            $this->pdo->setAttribute(attribute: PDO::SQLSRV_ATTR_ENCODING, value: PDO::SQLSRV_ENCODING_SYSTEM);
        } elseif (is_numeric(value: $charset)) {
            // charset code passed directly
            $this->pdo->setAttribute(attribute: PDO::SQLSRV_ATTR_ENCODING, value: $charset);
        } else {
            // unknown charset, use the default encoding
            $this->pdo->setAttribute(attribute: PDO::SQLSRV_ATTR_ENCODING, value: PDO::SQLSRV_ENCODING_DEFAULT);
        }
    }

    public static function buildDsn(array $config): string
    {
        $host = $config['host'] ?? 'localhost';
        $dbname = $config['dbname'] ?? '';
        return "sqlsrv:Server={$host};Database={$dbname}";
    }

    public string $driverName {
        get => 'sqlsrv';
    }
}
