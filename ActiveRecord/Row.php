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

use Qubus\Expressive\ActiveRecord\Exception\ReadOnlyException;
use Qubus\Expressive\OrmBuilder;

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
    public function __get($field)
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
    public function __set($field, $value)
    {
        $this->model->{$field} = $value;
    }

    public function __isset($field)
    {
        return !empty($this->model->{$field});
    }

    public function __call($name, $arguments)
    {
        if (method_exists(object_or_class: $this->model, method: $name)) {
            return call_user_func_array(callback: [$this->model, $name], args: $arguments);
        }
    }

    public function __toString(): string
    {
        $json = [];

        foreach ($this->model->getData() as $field => $value) {
            $json[$field] = $this->{$field};
        }

        return json_encode($json);
    }

    /**
     * @throws ReadOnlyException
     */
    public function save(): Model|int|bool|OrmBuilder
    {
        return $this->model->save();
    }

    /**
     * @throws ReadOnlyException
     */
    public function delete(): bool|int|OrmBuilder|null
    {
        return $this->model->delete();
    }
}