<?php

declare(strict_types=1);

namespace Qubus\Expressive\Migration\Adapter\Traits;

use Opis\Database\Database;
use PDO;
use Qubus\Exception\Exception;

use function array_map;
use function reset;

trait OpisListTablesAware
{
    protected function database(): Database
    {
        return new Database($this->connection);
    }

    /**
     * @throws Exception
     */
    protected function listTables(): array
    {
        $driver = $this->connection->getDriver();

        return match ($driver) {
            'mysql' => $this->mysqlListTables(),
            'sqlite' => $this->sqliteListTables(),
            'pgsql' => $this->pgsqlListTables(),
            'sqlsrv' => $this->sqlsrvListTables(),
            default => throw new Exception(message: 'List tables is not supported by this driver.'),
        };
    }

    protected function mysqlListTables(): array
    {
        $stmt = $this->database()->getConnection()->getPDO()->query(query: "SHOW TABLES");
        $result = $stmt->fetchAll(mode: PDO::FETCH_ASSOC);

        return array_map(callback: function ($r) {
            return reset($r);
        }, array: $result);
    }

    protected function sqliteListTables(): array
    {
        $stmt = $this->database()
                ->getConnection()
                ->getPDO()
                ->query(
                    query: "SELECT name FROM sqlite_master WHERE type = 'table'"
                    . " AND name != 'sqlite_sequence' AND name != 'geometry_columns'"
                    . " AND name != 'spatial_ref_sys' "
                    . "UNION ALL SELECT name FROM sqlite_temp_master "
                    . "WHERE type = 'table' ORDER BY name"
                );
        $result = $stmt->fetchAll(mode: PDO::FETCH_ASSOC);

        return array_map(callback: function ($i) {
            return reset($i);
        }, array: $result);
    }

    protected function pgsqlListTables(): array
    {
        $stmt = $this->database()
                ->getConnection()
                ->getPDO()
                ->query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'");
        $result = $stmt->fetchAll(mode: PDO::FETCH_ASSOC);

        return array_map(callback: function ($r) {
            return reset($r);
        }, array: $result);
    }

    protected function sqlsrvListTables(): array
    {
        $stmt = $this->database()
                ->getConnection()
                ->getPDO()
                ->query("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES");
        $result = $stmt->fetchAll(mode: PDO::FETCH_ASSOC);

        return array_map(callback: function ($r) {
            return reset($r);
        }, array: $result);
    }
}
