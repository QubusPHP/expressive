<?php

declare(strict_types=1);

namespace Qubus\Expressive\ActiveRecord\Relations;

use Qubus\Expressive\ActiveRecord\Model;

class BelongsTo extends Relation
{
    protected int|string|null $foreignKey = null;

    public function __construct(Model $parent, Model $related, string|int|null $foreignKey = null)
    {
        parent::__construct(parent: $parent, related: $related);

        $this->foreignKey = $foreignKey;
    }

    public function setJoin(): mixed
    {
        return $this->eagerLoading
        ? $this->related->whereIn((string) $this->related->getPrimaryKey(), (array) $this->eagerKeys)
        : $this->related->where($this->related->getPrimaryKey(), $this->parent->getData(field: $this->foreignKey));
    }

    public function match(Model $parent): mixed
    {
        return array_find(
            $this->eagerResults,
            fn($row) => $parent->{$this->foreignKey} === $row->getData($row->getPrimaryKey())
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
