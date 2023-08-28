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

namespace Qubus\Expressive\ActiveRecord;

use ArrayIterator;
use Countable;
use IteratorAggregate;
use Qubus\Expressive\OrmBuilder;

use const JSON_PRETTY_PRINT;

class Result implements Countable, IteratorAggregate
{
    protected ?Model $model = null;
    protected ?OrmBuilder $query = null;

    protected array $rows;

    public function __construct(Model $model, OrmBuilder $query = null)
    {
        $this->model = $model;
        $this->query = $query;
    }

    public function row()
    {
        if ($this->query === false) {
            return;
        }

        return new Row(model: $this->model, rowObject: $this->query);
    }

    public function rows(): static
    {
        $this->rows = [];
        if ($this->query === false) {
            return $this;
        }

        $class = get_class(object: $this->model);

        foreach ($this->query as $rowData) {
            $model = new $class();
            $model->exists = true;

            $newRow = new Row(model: $model, rowObject: $rowData);
            $this->rows[] = $newRow;
        }

        return $this;
    }

    // Alias for row();
    public function first(): ?Row
    {
        return $this->row();
    }

    public function pluck($field)
    {
        $first = $this->row();

        return $first->{$field};
    }

    // Eager loading
    public function load($method)
    {
        if (!is_callable(value: [$this->model, $method])) {
            return false;
        }

        $relation = call_user_func(callback: [$this->model, $method]);

        $primaries = [];

        foreach ($this->rows as $row) {
            $primaries[] = $row->getData($row->getPrimaryKey());
        }

        $this->rows = $relation->eagerLoad($this->rows, $primaries, $method);
    }

    public function toArray(): array
    {
        $array = [];

        foreach ($this->rows as $row) {
            $array[] = $row->toArray();
        }

        return $array;
    }

    public function toJson(): bool|string
    {
        return json_encode(value: $this->toArray(), flags: JSON_PRETTY_PRINT);
    }

    // Implements IteratorAggregate function
    public function getIterator(): ArrayIterator
    {
        return new ArrayIterator(array: $this->rows);
    }

    // Implements Countable function
    public function count(): int
    {
        return count($this->rows);
    }
}
