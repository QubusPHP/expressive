<?php

declare(strict_types=1);

namespace Qubus\Expressive\DataMapper;

use PDO;
use PDOStatement;
use Qubus\Expressive\Connection;
use Qubus\Expressive\QueryBuilder;
use ReflectionClass;
use ReflectionException;
use ReflectionProperty;

use function array_keys;
use function in_array;
use function rtrim;
use function sprintf;
use function strtoupper;

class PdoDataMapper implements DataMapper
{
    /** @var class-string<SerializableEntity> $entity */
    protected string $entity;

    /** @var string $table */
    protected string $table;

    /** @var array<string, string> $columns */
    protected array $columns = [];

    /** @var array<string, ReflectionProperty> */
    private array $properties = [];

    /**
     * @throws ReflectionException
     * @throws DataMapperException
     */
    public function __construct(public readonly Connection $connection, string $entity)
    {
        if (! is_subclass_of($entity, SerializableEntity::class)) {
            throw new DataMapperException(message: sprintf(
                'Entity %s must extend %s.',
                $entity,
                SerializableEntity::class
            ));
        }

        /** @var class-string<SerializableEntity> $entity */
        $this->entity = $entity;

        $reflection = new ReflectionClass($entity);

        $entityAttributes = $reflection->getAttributes(Entity::class);

        if (empty($entityAttributes [0])) {
            throw new DataMapperException(message: 'Invalid entity class.');
        }

        $table = $entityAttributes[0]->newInstance()->getTable();
        if ($table === null || $table === '') {
            throw new DataMapperException(message: sprintf('Entity %s must define a table.', $entity));
        }
        $this->table = $table;

        $properties = $reflection->getProperties();

        foreach ($properties as $property) {
            $propertyAttributes = $property->getAttributes(Property::class);

            if (empty($propertyAttributes[0])) {
                throw new DataMapperException(message: sprintf('Invalid properties for entity %s', $entity));
            }

            if (! $property->isPublic() || $property->isStatic()) {
                throw new DataMapperException(message: sprintf(
                    'Mapped property %s::$%s must be public and non-static.',
                    $entity,
                    $property->name
                ));
            }

            $propertyAttributes = $propertyAttributes[0]->newInstance();
            $this->columns[$property->name] = $propertyAttributes->getColumn();
            $this->properties[$property->name] = $property;
        }

        if (! isset($this->columns['id'])) {
            throw new DataMapperException(message: sprintf('Entity %s must define an id property.', $entity));
        }
    }

    /**
     * @return PDO
     */
    public function getPdo(): PDO
    {
        return $this->connection->pdo;
    }

    /**
     * @return QueryBuilder
     */
    public function queryBuilder(): QueryBuilder
    {
        return $this->connection->queryBuilder()->table($this->table);
    }

    /**
     * @param list<array<string, mixed>> $data
     * @return array<int|string, SerializableEntity>
     * @throws DataMapperException
     */
    public function hydrate(array $data): array
    {
        $objects = [];
        foreach ($data as $d) {
            $objects[$d[$this->columns['id']]] = $this->mapRowToObject($d);
        }

        return $objects;
    }

    /**
     * @param string $orderBy
     * @param array{direction?: string, limit?: int, offset?: int} $options
     * @return array<int|string, SerializableEntity>
     * @throws DataMapperException
     */
    public function findAll(string $orderBy = '', array $options = []): array
    {
        $sql = $this->buildSelectString();
        $sql .= $this->buildOrderByString($orderBy, $options['direction'] ?? 'ASC');
        $sql .= $this->buildLimitOffsetString($options['limit'] ?? 10, $options['offset'] ?? 0);

        $statement = $this->connection->pdo->query($sql);
        if (! $statement instanceof PDOStatement) {
            throw new DataMapperException(message: 'Unable to execute entity query.');
        }
        $rows = $statement->fetchAll(mode: PDO::FETCH_ASSOC);

        $objects = [];
        foreach ($rows as $row) {
            $objects[$row[$this->columns['id']]] = $this->mapRowToObject($row);
        }

        return $objects;
    }

    /**
     * @param string $column
     * @param string $value
     * @param string $orderBy
     * @param array{direction?: string, limit?: int, offset?: int} $options
     * @return array<int|string, SerializableEntity>
     * @throws DataMapperException
     */
    public function findAllBy(string $column, string $value, string $orderBy = '', array $options = []): array
    {
        $sql = $this->buildSelectString();
        $sql .= ' WHERE ';
        $sql .= $this->quotedColumn($column);
        $sql .= ' = :' . $column;
        $sql .= $this->buildOrderByString(orderBy: $orderBy, direction: $options['direction'] ?? 'ASC');
        $sql .= $this->buildLimitOffsetString(limit: $options['limit'] ?? 10, offset: $options['offset'] ?? 0);

        $stmt = $this->connection->pdo->prepare(query: $sql);
        $stmt->execute(params: [':' . $column => $value]);
        $rows = $stmt->fetchAll(mode: PDO::FETCH_ASSOC);

        $objects = [];
        foreach ($rows as $row) {
            $objects[$row[$this->columns['id']]] = $this->mapRowToObject(row: $row);
        }

        return $objects;
    }

    /**
     * @param int|string $id
     * @return SerializableEntity|null
     * @throws DataMapperException
     */
    public function findOne(int|string $id): ?SerializableEntity
    {
        $sql = $this->buildSelectString();
        $sql .= ' WHERE ';
        $sql .= $this->quotedColumn('id');
        $sql .= ' = :id';

        $stmt = $this->connection->pdo->prepare(query: $sql);
        $stmt->execute(params: [':id' => $id]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);

        return empty($row) ? null : $this->mapRowToObject($row);
    }

    /**
     * @param SerializableEntity $entity
     * @return SerializableEntity
     * @throws DataMapperException
     */
    public function create(SerializableEntity $entity): SerializableEntity
    {
        $properties = $this->insertProperties($entity);
        $sql = $this->buildInsertString($properties);

        $stmt = $this->connection->pdo->prepare(query: $sql);
        foreach ($properties as $column) {
            $stmt->bindValue(param: ':' . $column, value: $this->propertyValue($entity, $column));
        }
        $stmt->execute();

        if (! in_array('id', $properties, true)) {
            $this->properties['id']->setValue($entity, $this->connection->pdo->lastInsertId());
        }

        return $entity;
    }

    /**
     * @param SerializableEntity $entity
     * @return SerializableEntity
     * @throws DataMapperException
     */
    public function update(SerializableEntity $entity): SerializableEntity
    {
        $sql = $this->buildUpdateString();

        $stmt = $this->connection->pdo->prepare(query: $sql);
        foreach (array_keys($this->columns) as $column) {
            $stmt->bindValue(param: ':' . $column, value: $this->propertyValue($entity, $column));
        }
        $stmt->execute();

        return $entity;
    }

    /**
     * @param int|string $id
     * @return void
     * @throws DataMapperException
     */
    public function delete(int|string $id): void
    {
        $sql = $this->buildDeleteString();

        $stmt = $this->connection->pdo->prepare(query: $sql);
        $stmt->execute([':id' => $id]);
    }

    /**
     * @return string
     * @throws DataMapperException
     */
    private function buildSelectString(): string
    {
        $sql = 'SELECT ';

        foreach (array_keys($this->columns) as $property) {
            $sql .= $this->quotedColumn($property) . ', ';
        }
        $sql = rtrim($sql, ', ');
        $sql .= ' FROM ';
        $sql .= $this->connection->quoteIdentifier($this->table);

        return $sql;
    }

    /**
     * @param string $orderBy
     * @param string $direction
     * @return string
     * @throws DataMapperException
     */
    private function buildOrderByString(string $orderBy, string $direction = 'ASC'): string
    {
        $direction = strtoupper($direction);
        if (! in_array($direction, ['ASC', 'DESC'], true)) {
            throw new DataMapperException(message: 'Sort direction must be ASC or DESC.');
        }

        $sql = ' ORDER BY ';
        $sql .= $this->quotedColumn($orderBy === '' ? 'id' : $orderBy);
        $sql .= sprintf(' %s ', $direction);

        return $sql;
    }

    /**
     * @param int $limit
     * @param int $offset
     * @return string
     * @throws DataMapperException
     */
    private function buildLimitOffsetString(int $limit = 0, int $offset = 0): string
    {
        if ($limit < 0 || $offset < 0) {
            throw new DataMapperException(message: 'Limit and offset must be non-negative integers.');
        }

        $sql = ' LIMIT ';
        $sql .= $limit;
        $sql .= ' OFFSET ';
        $sql .= $offset;

        return $sql;
    }

    /**
     * @param list<string> $properties
     * @return string
     * @throws DataMapperException
     */
    private function buildInsertString(array $properties): string
    {
        if ($properties === []) {
            throw new DataMapperException('The entity has no initialized properties to insert.');
        }

        $sql = 'INSERT INTO ';
        $sql .= $this->connection->quoteIdentifier($this->table);
        $sql .= ' (';

        foreach ($properties as $property) {
            $sql .= $this->quotedColumn($property) . ', ';
        }
        $sql = rtrim($sql, ', ');

        $sql .= ' ) VALUES (';

        foreach ($properties as $column) {
            $sql .= ':' . $column . ', ';
        }
        $sql = rtrim($sql, ', ');

        $sql .= ')';

        return $sql;
    }

    /**
     * @return string
     * @throws DataMapperException
     */
    private function buildUpdateString(): string
    {
        $sql = 'UPDATE ';
        $sql .= $this->connection->quoteIdentifier($this->table);
        $sql .= ' SET ';

        foreach ($this->columns as $objectPropertyName => $dbFieldName) {
            if ($objectPropertyName === 'id') {
                continue;
            }
            $sql .= $this->connection->quoteIdentifier($dbFieldName) . ' = :' . $objectPropertyName . ', ';
        }
        $sql = rtrim($sql, ', ');

        $sql .= ' WHERE ';
        $sql .= $this->quotedColumn('id');
        $sql .= ' = :id';

        return $sql;
    }

    /**
     * @return string
     * @throws DataMapperException
     */
    private function buildDeleteString(): string
    {
        $sql = 'DELETE FROM ';
        $sql .= $this->connection->quoteIdentifier($this->table);
        $sql .= ' WHERE ';
        $sql .= $this->quotedColumn('id');
        $sql .= ' = :id';

        return $sql;
    }

    /**
     * @param array<string, mixed> $row
     * @return SerializableEntity
     * @throws DataMapperException
     */
    private function mapRowToObject(array $row): SerializableEntity
    {
        $object = new $this->entity();
        foreach ($this->columns as $objectPropertyName => $dbFieldName) {
            if (! array_key_exists($dbFieldName, $row)) {
                throw new DataMapperException(message: sprintf(
                    'Column %s is missing from the entity result.',
                    $dbFieldName
                ));
            }
            $this->properties[$objectPropertyName]->setValue($object, $row[$dbFieldName]);
        }

        return $object;
    }

    /**
     * @param string $property
     * @return string
     * @throws DataMapperException
     */
    private function quotedColumn(string $property): string
    {
        if (! isset($this->columns[$property])) {
            throw new DataMapperException(message: sprintf('Unknown entity property: %s.', $property));
        }

        return $this->connection->quoteIdentifier($this->columns[$property]);
    }

    /** @return list<string> */
    private function insertProperties(SerializableEntity $entity): array
    {
        $properties = [];

        foreach (array_keys($this->columns) as $property) {
            if ($property === 'id' && ! $this->properties[$property]->isInitialized($entity)) {
                continue;
            }

            $properties[] = $property;
        }

        return $properties;
    }

    /**
     * @param SerializableEntity $entity
     * @param string $property
     * @return mixed
     * @throws DataMapperException
     */
    private function propertyValue(SerializableEntity $entity, string $property): mixed
    {
        if (! $this->properties[$property]->isInitialized($entity)) {
            throw new DataMapperException(message: sprintf('Entity property %s is not initialized.', $property));
        }

        return $this->properties[$property]->getValue($entity);
    }
}
