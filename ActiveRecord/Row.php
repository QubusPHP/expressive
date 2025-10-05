<?php

declare(strict_types=1);

namespace Qubus\Expressive\ActiveRecord;

use Qubus\Expressive\ActiveRecord\Exception\ReadOnlyException;
use Qubus\Expressive\QueryBuilder;

use function call_user_func_array;
use function method_exists;
use function Qubus\Support\Helpers\is_null__;

class Row
{
    protected ?Model $model = null;

    public function __construct(Model $model, mixed $rowObject)
    {
        $this->model = $model;

        $this->model->exists = true;
        $this->model->setData(field: $rowObject);
    }

    // Getter
    public function __get($field): mixed
    {
        // Are we trying to get a related model?
        if (method_exists(object_or_class: $this->model, method: $field)) {
            // Is it eager loaded?
            $related = $this->model->getRelation(name: $field);
            if (!is_null__(var: $related)) {
                return $related;
            }

            $relation = call_user_func(callback: [$this->model, $field]);

            $data = $relation->getResults();
            return $data ?: [];
        }

        return $this->model->{$field};
    }

    // Setter
    public function __set($field, $value): void
    {
        $this->model->{$field} = $value;
    }

    public function __isset($field): bool
    {
        return !empty($this->model->{$field});
    }

    public function __call($name, $arguments): mixed
    {
        return method_exists(object_or_class: $this->model, method: $name)
        ? call_user_func_array(callback: [$this->model, $name], args: $arguments)
        : $this->model;
    }

    public function __toString(): string
    {
        $json = [];

        foreach ($this->model->getData() as $field => $value) {
            $json[$field] = $this->{$field};
        }

        return json_encode($json);
    }

    public function save(): Model|int|bool|QueryBuilder
    {
        try {
            return $this->model->save();
        } catch (ReadOnlyException $e) {
            error_log($e->getMessage());
        } finally {
            return false;
        }
    }

    public function delete(): bool|int|QueryBuilder|null
    {
        try {
            return $this->model->delete();
        } catch (ReadOnlyException $e) {
            error_log($e->getMessage());
        } finally {
            return false;
        }
    }
}
