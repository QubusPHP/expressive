<?php

declare(strict_types=1);

namespace Qubus\Expressive\Connection\Pdo;

use PDO;
use Qubus\Expressive\Connection\PdoConnection;

use function array_map;
use function reset;

class Pgsql extends PdoConnection
{
    public static function buildDsn(array $config): string
    {
        $host = $config['host'] ?? 'localhost';
        $port = $config['port'] ?? 5432;
        $dbname = $config['dbname'] ?? '';
        return "pgsql:host={$host};port={$port};dbname={$dbname}";
    }

    /**
     * Get an array of table names from the connection.
     *
     * @return  array  tables names.
     */
    public function listTables(): array
    {
        $query = $this->pdo->query(
            query: "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
        );
        $result = $query->fetchAll(mode: PDO::FETCH_ASSOC);

        return array_map(callback: function ($r) {
            return reset($r);
        }, array: $result);
    }

    /**
     * Get an array of database names from the connection.
     *
     * @return  array  database names.
     */
    public function listDatabases(): array
    {
        $query = $this->pdo->query(
            query: "SELECT datname FROM pg_database"
        );
        $result = $query->fetchAll(mode: PDO::FETCH_ASSOC);

        return array_map(callback: function ($r) {
            return reset($r);
        }, array: $result);
    }

    /**
     * Get an array of table fields from a table.
     *
     * @return  array  field arrays
     */
    public function listFields($table): array
    {
        $query = $this->pdo->query(
            query: "SELECT * FROM information_schema.columns WHERE table_name = :table"
        );
        $query->bindValue(param: ':table', value: $table);
        $query->execute();

        $result = $query->fetchAll(mode: PDO::FETCH_ASSOC);

        return array_map(callback: function ($r) {
            return reset($r);
        }, array: $result);
    }

    public string $driverName {
        get => 'pgsql';
    }
}
