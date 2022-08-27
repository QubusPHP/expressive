<?php

/**
 * Qubus\Expressive
 *
 * @link       https://github.com/QubusPHP/expressive
 * @copyright  2022
 * @author     Joshua Parker <joshua@joshuaparker.dev>
 * @license    https://opensource.org/licenses/mit-license.php MIT License
 *
 * @since      0.1.0
 */

declare(strict_types=1);

namespace Qubus\Expressive\ActiveRecord;

use DateTimeImmutable;
use DateTimeZone;
use Exception;
use Qubus\Dbal\Connection;
use Qubus\Expressive\ActiveRecord\Exception\ReadOnlyException;
use Qubus\Expressive\ActiveRecord\Relations\BelongsTo;
use Qubus\Expressive\ActiveRecord\Relations\BelongsToMany;
use Qubus\Expressive\ActiveRecord\Relations\HasMany;
use Qubus\Expressive\ActiveRecord\Relations\HasOne;
use Qubus\Expressive\ActiveRecord\Relations\Relation;
use Qubus\Expressive\OrmBuilder;

use function get_called_class;
use function is_array;
use function Qubus\Support\Helpers\camel_case;
use function Qubus\Support\Helpers\is_null__;
use function Qubus\Support\Helpers\studly_case;

use const JSON_PRETTY_PRINT;

class Model
{
    /**
     * Date format to use for database.
     */
    public const DATE_FORMAT = 'Y-m-d H:i:s.u';
    /**
     * Database connection.
     */
    protected static ?Connection $connection = null;
    /**
     * Default orm query builder.
     */
    protected ?OrmBuilder $queryBuilder = null;
    /**
     * Database table name.
     */
    protected ?string $tableName = null;
    /**
     * Database table prefix.
     */
    protected ?string $tablePrefix = null;
    /**
     * Parent key found in related model.
     */
    protected string $foreignKey = '%s_id';
    /**
     * Primary key of parent model.
     */
    protected string $primaryKey = 'id';

    protected bool $incrementing = true;

    public bool $exists = false;
    /**
     * Query result data.
     */
    protected array $data = [];
    /**
     * To stored loaded relation.
     */
    protected array $relations = [];
    /**
     * Whitelist of attributes that are checked for mass assignment.
     */
    protected array $fillable = [];
    /**
     * Blacklist of attributes that cannot be mass-assigned.
     */
    protected array $guarded = [];
    /**
     * Flag of whether fillable/guarded attributes should be guarded.
     */
    protected bool $guardFlag = true;
    /**
     * Sets whether model is read only.
     */
    protected bool $isReadOnly = false;


    public function __construct(array $newData = [])
    {
        if (is_array(value: $newData)) {
            $this->setData(field: $newData);
        }
    }

    public static function connection(Connection $connection): Connection
    {
        return self::$connection = $connection;
    }

    protected function ormQuery(): OrmBuilder
    {
        return OrmBuilder::fromInstance(
            connection: self::$connection,
            table: $this->tableName,
            primaryKeyName: $this->primaryKey,
            tablePrefix: $this->tablePrefix
        );
    }

    protected function query(): static
    {
        $this->ormQuery();

        return $this;
    }

    protected function all(string|array $columns = '*'): Result
    {
        $builder = $this->ormQuery()->select(columns: $columns);

        $result = new Result(model: $this, query: $builder);
        return $result->rows();
    }

    protected function get(string|array $columns = '*'): Result
    {
        if (is_null__(var: $this->queryBuilder)) {
            return $this->all(columns: $columns);
        }

        $this->queryBuilder->select(columns: $columns);

        $result = new Result(model: $this, query: $this->queryBuilder);
        return $result->rows();
    }

    protected function first(string|array $columns = '*'): ?Row
    {
        $builder = $this->ormQuery()->select(columns: $columns)->findOne();

        $result = new Result(model: $this, query: $builder);
        return $result->first();
    }

    protected function find(mixed $id): Result|Row|null
    {
        $args = func_get_args();
        if (count($args) > 1) {
            $id = [];
            foreach ($args as $arg) {
                $id[] = $arg;
            }
        }

        if (is_array(value: $id)) {
            $builder = $this->ormQuery()->whereIn(
                columnName: $this->primaryKey,
                values: $id
            );
        } else {
            $builder = $this->ormQuery()->where(
                condition: $this->primaryKey . ' = ?',
                parameters: $id
            )->findOne();
        }

        $result = new Result($this, $builder);
        return is_array(value: $id) ? $result->rows() : $result->first();
    }

    protected function pluck($field)
    {
        $row = $this->first(columns: [$field]);

        return $row->{$field};
    }

    /**
     * @throws ReadOnlyException
     */
    protected static function create(array $data): bool|static
    {
        if (empty($data)) {
            return false;
        }

        $class = new static(newData: $data);
        $class->save();

        return $class;
    }

    /**
     * @throws ReadOnlyException
     */
    protected function update(array $data): bool|OrmBuilder|int
    {
        $this->isReadOnly(methodName: 'update');

        if (empty($this->queryBuilder)) {
            $param = func_get_args();
            if (count($param) < 1) {
                return false;
            }

            [$data, $where] = $param;

            return $this->ormQuery()->update(data: $data)->where(condition: $where);
        } else {
            return $this->queryBuilder->update(data: $data);
        }
    }

    /**
     * @throws ReadOnlyException
     */
    protected function save(): bool|int|OrmBuilder
    {
        $this->isReadOnly(methodName: 'save');

        if (empty($this->data)) {
            return false;
        }

        // Do an insert statement
        if (!$this->exists) {
            if (!$this->incrementing && empty($this->data[ $this->primaryKey ])) {
                return false;
            }

            $return = $this->ormQuery()->insert(data: $this->data);

            //if ($return !== false) {
            if ($return->rowCount() > 0) {
                $this->exists = true;

                if ($this->incrementing) {
                    $this->setData(field: $this->primaryKey, value: $return->lastInsertId());
                }
            }

            return $return;
        } else {
            $where = [$this->primaryKey => $this->getData(field: $this->primaryKey)];

            return $this->ormQuery()->update(data: $this->getData())->where(condition: $where);
        }
    }

    /**
     * @throws ReadOnlyException
     */
    protected function delete(): bool|OrmBuilder|int
    {
        $this->isReadOnly(methodName: 'delete');

        if (!$this->exists && empty($this->queryBuilder)) {
            $params = func_get_args();
            if (empty($params)) {
                return false;
            }

            $first = reset($params);
            if (is_array(value: $first)) {
                $params = $first;
            }

            $where = [];

            foreach ($params as $id) {
                if (is_array(value: $id)) {
                    continue;
                }
                $where[] = $id;
            }

            if (count($where) <= 1) {
                $builder = $this->ormQuery()->where(condition: $this->primaryKey, parameters: reset($where));
            } else {
                $builder = $this->ormQuery()->whereIn(columnName: $this->primaryKey, values: $where);
            }

            return $builder->delete();
        }

        if ($this->exists) {
            $this->where($this->primaryKey, $this->getData($this->primaryKey));
        }

        return $this->queryBuilder->delete();
    }

    public function getPrimaryKey(): string
    {
        return $this->primaryKey;
    }

    public function getData($field = null)
    {
        return !empty($field) ? $this->data[$field] : $this->data;
    }

    public function setData(mixed $field, mixed $value = null): void
    {
        if (!is_null__(var: $value)) {
            $this->setAttributesViaMassAssignment(attributes: [$field => $value]);
        } else {
            $this->setAttributesViaMassAssignment(attributes: $field);
        }
    }

    public function toArray(): array
    {
        $array = $this->data;

        foreach ($this->relations as $relation => $models) {
            foreach ($models as $model) {
                $array[ $relation ][] = $model->toArray();
            }
        }

        return $array;
    }

    public function toJson(): bool|string
    {
        return json_encode(value: $this->toArray(), flags: JSON_PRETTY_PRINT);
    }

    /**
     * ======================================
     * Relationship Methods
     * ======================================
     */

    public function hasOne(Model|string $related, string|int|null $foreignKey = null): HasOne
    {
        if (empty($foreignKey)) {
            $foreignKey = strtolower(string: get_called_class()) . '_id';
        }

        return new HasOne(parent: $this, related: new $related(), foreignKey: $foreignKey);
    }

    public function hasMany(Model|string $related, string|int|null $foreignKey = null): HasMany
    {
        if (empty($foreignKey)) {
            $foreignKey = strtolower(string: get_called_class()) . '_id';
        }

        return new HasMany(parent: $this, related: new $related(), foreignKey: $foreignKey);
    }

    public function belongsTo(Model|string $related, string|int|null  $foreignKey = null): BelongsTo
    {
        if (is_null__(var: $foreignKey)) {
            $foreignKey = strtolower(string: $related) . '_id';
        }

        return new BelongsTo(parent: $this, related: new $related(), foreignKey: $foreignKey);
    }

    public function belongsToMany(
        Model|string $related,
        ?string $pivotTable = null,
        string|int|null $foreignKey = null,
        string|int|null $otherKey = null
    ): BelongsToMany {
        if (empty($pivotTable)) {
            $models = [strtolower(string: get_called_class()), strtolower(string: $related)];
            sort($models);

            $pivotTable = strtolower(string: implode(separator: '_', array: $models));
        }

        if (empty($foreignKey)) {
            $foreignKey = strtolower(string: get_called_class()) . '_id';
        }

        if (empty($otherKey)) {
            $otherKey = strtolower(string: $related) . '_id';
        }

        $pivotBuilder = OrmBuilder::fromInstance(connection: self::$connection, table: $pivotTable);

        return new BelongsToMany(
            parent: $this,
            related: new $related(),
            pivotBuilder: $pivotBuilder,
            foreignKey: $foreignKey,
            otherKey: $otherKey
        );
    }

    public function setRelation($name, Relation $relation): void
    {
        $this->relations[ $name ] = $relation->relate(parent: $this);
    }

    public function getRelation($name)
    {
        return $this->relations[$name] ?? null;
    }

    // Eager loading for a single row? Just call the method
    public function load(string $related): void
    {
        if (!method_exists(object_or_class: $this, method: $related)) {
            return;
        }

        $this->setRelation(name: $related, relation: $this->$related());
    }

    // ======================================
    // Aggregate Methods
    // ======================================

    protected function aggregates(mixed $function, mixed $field)
    {
        if (empty($this->queryBuilder)) {
            $this->queryBuilder = $this->ormQuery();
        }

        return $this->queryBuilder->{$function}($field);
    }

    protected function max(mixed $field)
    {
        return $this->aggregates(function: __FUNCTION__, field: $field);
    }

    protected function min(mixed $field)
    {
        return $this->aggregates(function: __FUNCTION__, field: $field);
    }

    protected function avg(mixed $field): float
    {
        return round($this->aggregates(function: __FUNCTION__, field: $field), 2);
    }

    protected function sum(mixed $field)
    {
        return $this->aggregates(function: __FUNCTION__, field: $field);
    }

    protected function count(mixed $field = null)
    {
        if (empty($field)) {
            $field = $this->getPrimaryKey();
        }

        return $this->aggregates(function: __FUNCTION__, field: $field);
    }

    /**
     * ======================================
     * Mass assignment protection.
     * ======================================
     */

    protected function setAttributesViaMassAssignment(array|object $attributes): void
    {
        $fillable  = !empty($this->fillable);
        $guarded = !empty($this->guarded);

        foreach ($attributes as $field => $value) {
            if ($this->guardFlag) {
                if ($fillable && !in_array(needle: $field, haystack: $this->fillable)) {
                    continue;
                }

                if ($guarded && in_array(needle: $field, haystack: $this->guarded)) {
                    continue;
                }

                $this->data[$field] = $value;
            }
        }
    }

    /**
     * ======================================
     * Timestamps
     * ======================================
     */

    /**
     * @return void
     * @throws Exception
     */
    protected function timestamp(): void
    {
        $now = new DateTimeImmutable(datetime: 'now', timezone: new DateTimeZone(timezone: 'UTC'));

        if (isset($this->updatedAt)) {
            $this->updatedAt = (string) $now->format(format: self::DATE_FORMAT);
        }

        if (isset($this->createdAt) && !$this->exists) {
            $this->createdAt = (string) $now->format(format: self::DATE_FORMAT);
        }
    }

    /**
     * @throws ReadOnlyException
     */
    private function isReadOnly(string $methodName): void
    {
        if ($this->isReadOnly) {
            throw new ReadOnlyException(
                message: sprintf(
                    'The model `%s` is readonly and calling method `%s` is not allowed.',
                    get_called_class(),
                    $methodName
                )
            );
        }
    }

    /**
     * ======================================
     * Magic Methods
     * ======================================
     */

    public function __call(mixed $name, mixed $arguments)
    {
        // Check if the method is available in this model
        if (method_exists(object_or_class: $this, method: $name)) {
            return call_user_func_array(callback: [$this, $name], args: $arguments);
        }

        // Check if the method is a "scope" method
        // Read documentation about scope method
        $scope = 'scope' . studly_case(string: $name);

        if (method_exists(object_or_class: $this, method: $scope)) {
            array_unshift($arguments, $this);

            return call_user_func_array(callback: [$this, $scope], args: $arguments);
        }

        if (is_null__(var: $this->queryBuilder)) {
            $this->queryBuilder = $this->ormQuery();
        }

        if (is_callable(value: [$this->queryBuilder, $name])) {
            call_user_func_array(callback: [$this->queryBuilder, $name], args: $arguments);
            return $this;
        }

        //return show_error('Unknown function '.$name, 500);
    }

    public static function __callStatic(mixed $name, mixed $arguments)
    {
        $model = get_called_class();

        return call_user_func_array(callback: [new $model(), $name], args: $arguments);
    }

    public function __get(mixed $field)
    {
        if (!isset($this->data[$field])) {
            return null;
        }
        $value = $this->data[$field];

        $accessor = 'getAttr' . camel_case(str: $field);

        return method_exists(object_or_class: $this, method: $accessor)
            ? call_user_func([$this, $accessor], $value, $this) : $value;
    }

    public function __set(mixed $field, mixed $value)
    {
        $mutator = 'setAttr' . camel_case(str: $field);

        if (method_exists(object_or_class: $this, method: $mutator)) {
            $value = call_user_func([$this, $mutator], $value, $this);
        }

        $this->setData(field: $field, value: $value);
    }

    public function __isset(mixed $field)
    {
        return !empty($this->data[ $field ]);
    }
}
