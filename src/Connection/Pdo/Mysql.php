<?php

declare(strict_types=1);

namespace Qubus\Expressive\Connection\Pdo;

use Exception;
use PDO;
use Qubus\Expressive\Connection\PdoConnection;

use function sprintf;

class Mysql extends PdoConnection
{
    public static function buildDsn(array $config): string
    {
        $host = $config['host'] ?? 'localhost';
        $dbname = $config['dbname'] ?? '';
        $charset = $config['charset'] ?? 'utf8mb4';
        return "mysql:host={$host};dbname={$dbname};charset={$charset}";
    }

    /**
     * Get an array of table names from the connection.
     *
     * @return array tables names
     * @throws Exception
     */
    public function listTables(): array
    {
        $query = $this->pdo->query(query: "SHOW TABLES");
        $result = $query->fetchAll(mode: PDO::FETCH_ASSOC);

        return array_map(callback: function ($r) {
            return reset($r);
        }, array: $result);
    }

    /**
     * Get an array of database names from the connection.
     *
     * @return  array  database names
     * @throws Exception
     */
    public function listDatabases(): array
    {
        $query = $this->pdo->query(query: 'SHOW DATABASES');
        $result = $query->fetchAll(mode: PDO::FETCH_ASSOC);

        return array_map(callback: function ($r) {
            return reset($r);
        }, array: $result);
    }

    /**
     * Get an array of table fields from a table.
     *
     * @return array field arrays
     * @throws Exception
     */
    public function listFields($table): array
    {
        $query = $this->pdo->query(
            query: sprintf('SHOW FULL COLUMNS FROM %s', $this->quoteIdentifier(identifier: $table))
        );
        $result = $query->fetchAll(mode: PDO::FETCH_ASSOC);

        $return = [];

        foreach ($result as $r) {
            $type = $r['Type'];

            if (strpos(haystack: $type, needle: ' ')) {
                [$type, $extra] = explode(separator: ' ', string: $type, limit: 2);
            }

            if ($pos = strpos(haystack: $type, needle: '(')) {
                $field['type'] = substr(string: $type, offset: 0, length: $pos);
                $field['constraint'] = substr(string: $type, offset: $pos + 1, length: -1);
            } else {
                $field['constraint'] = null;
            }

            $field['extra'] = $extra ?? null;

            $field['name'] = $r['Field'];
            $field['default'] = $r['Default'];
            $field['null'] = $r['Null'] !== 'No';
            $field['privileges'] = explode(separator: ',', string: $r['Privileges']);
            $field['key'] = $r['Key'];
            $field['comments'] = $r['Comment'];
            $field['collation'] = $r['Collation'];

            if ($r['Extra'] === 'auto_increment') {
                $field['auto_increment'] = true;
            } else {
                $field['auto_increment'] = false;
            }

            $return[$field['name']] = $field;
        }

        return $return;
    }

    public string $driverName {
        get => 'mysql';
    }
}
