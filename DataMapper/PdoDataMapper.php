<?php

/**
 * Qubus\Expressive
 *
 * @link       https://github.com/QubusPHP/expressive
 * @copyright  2022
 * @author     Joshua Parker <joshua@joshuaparker.dev>
 * @license    https://opensource.org/licenses/mit-license.php MIT License
 */

declare(strict_types=1);

namespace Qubus\Expressive\DataMapper;

use PDO;
use ReflectionClass;
use ReflectionException;

use function array_keys;
use function rtrim;
use function sprintf;

class PdoDataMapper implements DataMapper
{
    /** @var string $entity */
    protected string $entity;

    /** @var string $table */
    protected string $table;

    /** @var array $columns */
    protected array $columns;

    /**
     * @throws ReflectionException
     */
    public function __construct(public readonly PDO $pdo, string $entity)
    {
        $this->entity = $entity;

        $reflection = new ReflectionClass($entity);

        $entityAttributes = $reflection->getAttributes(Entity::class);

        if (empty($entityAttributes [0])) {
            throw new DataMapperException(message: 'Invalid entity class.');
        }

        $this->table = $entityAttributes [0]->newInstance()->getTable();

        $properties = $reflection->getProperties();

        foreach ($properties as $property) {
            $propertyAttributes = $property->getAttributes(Property::class);

            if (empty($propertyAttributes[0])) {
                throw new DataMapperException(message: sprintf('Invalid properties for entity %s', $entity));
            }

            $propertyAttributes = $propertyAttributes[0]->newInstance();
            $this->columns[$property->name] = $propertyAttributes->getColumn();
        }
    }

    public function getPdo(): PDO
    {
        return $this->pdo;
    }

    public function findAll(string $orderBy = '', array $options = []): array
    {
        $sql = $this->buildSelectString();
        $sql .= $this->buildOrderByString($orderBy, $options['direction'] ?? 'ASC');
        $sql .= $this->buildLimitOffsetString($options['limit'] ?? 10, $options['offset'] ?? 0);

        $rows = $this->pdo->query($sql)->fetchAll(mode: PDO::FETCH_ASSOC);

        $objects = [];
        foreach ($rows as $row) {
            $objects[$row[$this->columns['id']]] = $this->mapRowToObject($row);
        }

        return $objects;
    }

    public function findAllBy(string $column, string $value, string $orderBy = '', array $options = []): array
    {
        $sql = $this->buildSelectString();
        $sql .= ' WHERE ';
        $sql .= $this->columns[$column];
        $sql .= ' = :' . $column;
        $sql .= $this->buildOrderByString(orderBy: $orderBy, direction: $options['direction'] ?? 'ASC');
        $sql .= $this->buildLimitOffsetString(limit: $options['limit'] ?? 10, offset: $options['offset'] ?? 0);

        $stmt = $this->pdo->prepare(query: $sql);
        $stmt->execute(params: [':' . $column => $value]);
        $rows = $stmt->fetchAll(mode: PDO::FETCH_ASSOC);

        $objects = [];
        foreach ($rows as $row) {
            $objects[$row[$this->columns['id']]] = $this->mapRowToObject(row: $row);
        }

        return $objects;
    }

    public function findOne(int|string $id): ?SerializableEntity
    {
        $sql = $this->buildSelectString();
        $sql .= ' WHERE ';
        $sql .= $this->columns['id'];
        $sql .= ' = :id';

        $stmt = $this->pdo->prepare(query: $sql);
        $stmt->execute(params: [':id' => $id]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);

        return empty($row) ? null : $this->mapRowToObject($row);
    }

    public function create(SerializableEntity $entity): SerializableEntity
    {
        $sql = $this->buildInsertString();

        $stmt = $this->pdo->prepare(query: $sql);
        foreach (array_keys($this->columns) as $column) {
            if ($column === 'id') {
                continue;
            }
            $stmt->bindValue(param: ':' . $column, value: $entity->$column);
        }
        $stmt->execute();

        $entity->id = $this->pdo->lastInsertId();

        return $entity;
    }

    public function update(SerializableEntity $entity): SerializableEntity
    {
        $sql = $this->buildUpdateString();

        $stmt = $this->pdo->prepare(query: $sql);
        foreach (array_keys($this->columns) as $column) {
            $stmt->bindValue(param: ':' . $column, value: $entity->$column);
        }
        $stmt->execute();

        return $entity;
    }

    public function delete(int|string $id): void
    {
        $sql = $this->buildDeleteString();

        $stmt = $this->pdo->prepare(query: $sql);
        $stmt->execute([':id' => $id]);
    }

    private function buildSelectString(): string
    {
        $sql = 'SELECT ';

        foreach ($this->columns as $column) {
            $sql .= $column . ', ';
        }
        $sql = rtrim($sql, ', ');
        $sql .= ' FROM ';
        $sql .= $this->table;

        return $sql;
    }

    private function buildOrderByString(string $orderBy, string $direction = 'ASC'): string
    {
        $sql = ' ORDER BY ';
        $sql .= empty($orderBy) ? $this->columns['id'] : $this->columns[$orderBy];
        $sql .= sprintf(' %s ', $direction);

        return $sql;
    }

    private function buildLimitOffsetString(int $limit = 0, int $offset = 0): string
    {
        $sql = ' LIMIT ';
        $sql .= $limit;
        $sql .= ' OFFSET ';
        $sql .= $offset;

        return $sql;
    }

    private function buildInsertString(): string
    {
        $sql = 'INSERT INTO ';
        $sql .= $this->table;
        $sql .= ' (';

        foreach ($this->columns as $objectPropertyName => $dbFieldName) {
            if ($objectPropertyName === 'id') {
                continue;
            }
            $sql .= $dbFieldName . ', ';
        }
        $sql = rtrim($sql, ', ');

        $sql .= ' ) VALUES (';

        foreach (array_keys($this->columns) as $column) {
            if ($column == 'id') {
                continue;
            }
            $sql .= ':' . $column . ', ';
        }
        $sql = rtrim($sql, ', ');

        $sql .= ')';

        return $sql;
    }

    private function buildUpdateString(): string
    {
        $sql = 'UPDATE ';
        $sql .= $this->table;
        $sql .= ' SET ';

        foreach ($this->columns as $objectPropertyName => $dbFieldName) {
            if ($objectPropertyName === 'id') {
                continue;
            }
            $sql .= $dbFieldName . ' = :' . $objectPropertyName . ', ';
        }
        $sql = rtrim($sql, ', ');

        $sql .= ' WHERE ';
        $sql .= $this->columns['id'];
        $sql .= ' = :id';

        return $sql;
    }

    private function buildDeleteString(): string
    {
        $sql = 'DELETE FROM ';
        $sql .= $this->table;
        $sql .= ' WHERE ';
        $sql .= $this->columns['id'];
        $sql .= ' = :id';

        return $sql;
    }

    private function mapRowToObject(array $row): SerializableEntity
    {
        $object = new $this->entity();
        foreach ($this->columns as $objectPropertyName => $dbFieldName) {
            $object->$objectPropertyName = $row[$dbFieldName];
        }

        return $object;
    }
}
