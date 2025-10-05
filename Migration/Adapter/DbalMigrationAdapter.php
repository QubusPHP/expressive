<?php

declare(strict_types=1);

namespace Qubus\Expressive\Migration\Adapter;

use Exception;
use Qubus\Expressive\Schema\CreateTable;
use Qubus\Expressive\Connection;
use Qubus\Expressive\Migration\Migration;
use Qubus\Support\DateTime\QubusDateTimeImmutable;

class DbalMigrationAdapter implements MigrationAdapter
{
    public function __construct(protected Connection $connection, protected string $tableName)
    {
    }

    public function connection(): Connection
    {
        return $this->connection;
    }

    /**
     * Get all migrated version numbers
     *
     * @return array
     * @throws Exception
     */
    public function fetchAll(): array
    {
        $tableName = $this->connection->quoteIdentifier($this->tableName);
        $sql = $this->connection->query(sql: "SELECT version FROM $tableName ORDER BY version ASC")->fetchAssoc();
        $all = $sql->all();

        return array_map(fn ($v) => $v['version'], $all);
    }

    /**
     * Up
     *
     * @param Migration $migration
     * @return MigrationAdapter
     */
    public function up(Migration $migration): MigrationAdapter
    {
        $this->connection->queryBuilder()
            ->table($this->tableName)
            ->insert([
                'version' => $migration->getVersion(),
                'recorded_on' => new QubusDateTimeImmutable(time: 'now')->format(format: 'Y-m-d h:i:s')
            ]);

        return $this;
    }

    /**
     * Down
     *
     * @param Migration $migration
     * @return MigrationAdapter
     */
    public function down(Migration $migration): MigrationAdapter
    {
        $this->connection->queryBuilder()
            ->table($this->tableName)
            ->where(condition: 'version', parameters: $migration->getVersion())
            ->delete();

        return $this;
    }

    /**
     * Is the schema ready?
     *
     * @return bool
     * @throws Exception
     */
    public function hasSchema(): bool
    {
        $tables = $this->connection->listTables();

        if (in_array(needle: $this->tableName, haystack: $tables)) {
            return true;
        }

        return false;
    }

    /**
     * Create Schema
     *
     * @return MigrationAdapter
     * @throws Exception
     */
    public function createSchema(): MigrationAdapter
    {
        $this->connection->getSchema()->create($this->tableName, function (CreateTable $table) {
            $table->integer(name: 'id')->size(value: 'big')->autoincrement();
            $table->string(name: 'version', length: 191)->notNull();
            $table->dateTime(name: 'recorded_on');
        });

        return $this;
    }
}
