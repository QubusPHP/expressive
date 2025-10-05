<?php

declare(strict_types=1);

namespace Qubus\Expressive\ActiveRecord\Relations;

use Countable;
use EmptyIterator;
use Iterator;
use IteratorAggregate;
use Qubus\Expressive\ActiveRecord\Model;
use Qubus\Expressive\ActiveRecord\Result;
use Qubus\Expressive\ActiveRecord\Row;

abstract class Relation implements Countable, IteratorAggregate
{
    protected ?Model $parent = null;
    protected ?Model $related = null;

    protected mixed $join = null;

    protected bool $eagerLoading = false;
    protected array $eagerKeys = [];
    protected mixed $eagerResults = null;

    public function __construct(Model $parent, Model $related)
    {
        $this->parent = $parent;
        $this->related = $related;
    }

    abstract public function getResults(): mixed;

    abstract public function setJoin(): mixed;

    abstract public function match(Model $parent): mixed;

    public function eagerLoad($parentRows, $relatedKeys, $relation): mixed
    {
        $this->eagerLoading = true;
        $this->eagerKeys = (array) $relatedKeys;

        foreach ($parentRows as $i => $row) {
            $row->setRelation($relation, $this);

            $parentRows[$i] = $row;
        }

        return $parentRows;
    }

    public function relate(Model $parent): mixed
    {
        if (empty($this->eagerResults)) {
            if (empty($this->join)) {
                $this->join = $this->setJoin();
            }

            $this->eagerResults = $this->join->get();
        }

        return $this->match(parent: $parent);
    }

    // Implements IteratorAggregate function so the result can be looped without needs to call get() first.
    public function getIterator(): Result|Iterator
    {
        $return = $this->getResults();

        return ($return instanceof Result) ? $return : new EmptyIterator();
    }

    // Implements Countable function
    public function count(): int
    {
        $result = $this->getResults();

        return ($result instanceof Result) ? count($this->getResults()) : 0;
    }

    // Chains with Active Record method if available
    public function __call(mixed $name, mixed $param): mixed
    {
        if (is_callable(value: [$this->related, $name])) {
            if (empty($this->join)) {
                $parentData = $this->parent->getData();

                // If parent data is empty then it means we are eager loading.
                if (!empty($parentData)) {
                    $this->join = $this->setJoin();
                } else { // No need to generate the "join", it will be generated later with eager loading method
                    $this->join = $this->related;
                }
            }

            $return = call_user_func_array(callback: [$this->join, $name], args: $param);

            if ($return instanceof Result || $return instanceof Row) {
                return $return;
            } elseif ($name === 'get') {
                return new EmptyIterator();
            }
        }

        return $this;
    }
}
