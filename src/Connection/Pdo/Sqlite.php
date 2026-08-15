<?php

declare(strict_types=1);

namespace Qubus\Expressive\Connection\Pdo;

use PDO;
use Qubus\Expressive\Connection\PdoConnection;

use function array_map;
use function reset;
use function strpos;
use function substr;

class Sqlite extends PdoConnection
{
    /**
     * Sets the connection encoding.
     *
     * @param string $charset  encoding
     */
    public function setCharset(string $charset): void
    {
        if ($charset) {
            $this->pdo->exec(statement: 'PRAGMA encoding = ' . $this->quote(value: $charset));
        }
    }

    public function listTables(): array
    {
        return array_map(callback: function ($i) {
            return reset($i);
        }, array: $this->pdo->query(query: "SELECT name FROM sqlite_master WHERE type = 'table'"
            . " AND name != 'sqlite_sequence' AND name != 'geometry_columns'"
            . " AND name != 'spatial_ref_sys' "
            . "UNION ALL SELECT name FROM sqlite_temp_master "
            . "WHERE type = 'table' ORDER BY name")
            ->fetchAll(mode: PDO::FETCH_ASSOC));
    }

    public function listFields(mixed $table): array
    {
        return array_map(callback: function ($i) {
            $field = [
                'name' => $i['name'],
            ];

            $type = $i['type'];

            if ($pos = strpos(haystack: $type, needle: '(')) {
                $field['type'] = substr(string: $type, offset: 0, length: $pos);
                $field['constraint'] = substr(string: $type, offset: $pos + 1, length: -1);
            } else {
                $field['constraint'] = null;
            }

            $field['null'] = ! (bool) $i['notnull'];
            $field['default'] = $i['dflt_value'];
            $field['primary'] = (bool) $i['pk'];

            return $field;
        }, array: $this->pdo->query(query: 'Pragma table_info(' . $this->quoteIdentifier(identifier: $table) . ')')
            ->fetchAll(mode: PDO::FETCH_ASSOC));
    }

    public static function buildDsn(array $config): string
    {
        $path = $config['path'] ?? $config['dbname'] ?? ':memory:';
        return "sqlite:{$path}";
    }

    public string $driverName {
    get => 'sqlite';
    }
}
