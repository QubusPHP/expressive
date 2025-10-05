<?php

declare(strict_types=1);

namespace Qubus\Expressive\ActiveRecord\Relations;

use Qubus\Expressive\ActiveRecord\Model;

class HasOne extends Relation
{
    protected int|string|null $foreignKey = null;

    public function __construct(Model $parent, Model $related, string|int|null $foreignKey = null)
    {
        parent::__construct($parent, $related);

        $this->foreignKey = $foreignKey;
    }

    public function setJoin(): mixed
    {
        return $this->eagerLoading
            ? $this->related->whereIn($this->foreignKey, $this->eagerKeys)
            : $this->related->where($this->foreignKey, $this->parent->getData(field: $this->parent->getPrimaryKey()));
    }

    public function match(Model $parent): mixed
    {
        return array_find(
                $this->eagerResults,
                fn($row) => $row->{$this->foreignKey} === $parent->getData(field: $parent->getPrimaryKey())
        );
    }

    public function getResults(): mixed
    {
        if (empty($this->join)) {
            $this->join = $this->setJoin();
        }

        return $this->join->first();
    }
}
