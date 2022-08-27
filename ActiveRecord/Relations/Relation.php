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
    protected Model $parent;
    protected Model $related;

    protected mixed $join;

    protected bool $eagerLoading = false;
    protected array $eagerKeys;
    protected mixed $eagerResults;

    public function __construct(Model $parent, Model $related)
    {
        $this->parent = $parent;
        $this->related = $related;
    }

    abstract public function getResults();

    abstract public function setJoin();

    abstract public function match(Model $parent);

    public function eagerLoad($parentRows, $relatedKeys, $relation)
    {
        $this->eagerLoading = true;
        $this->eagerKeys = (array) $relatedKeys;

        foreach ($parentRows as $i => $row) {
            $row->setRelation($relation, $this);

            $parentRows[$i] = $row;
        }

        return $parentRows;
    }

    public function relate(Model $parent)
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
    public function __call(mixed $name, mixed $param)
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

            return $this;
        }
    }
}
