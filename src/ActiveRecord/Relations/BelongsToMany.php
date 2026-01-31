<?php

declare(strict_types=1);

namespace Qubus\Expressive\ActiveRecord\Relations;

use Qubus\Expressive\ActiveRecord\Model;

class BelongsToMany extends Relation
{
    protected ?string $pivotBuilder = null;
    protected mixed $pivotResult = null;

    protected string|int|null $foreignKey = null;
    protected string|int|null $otherKey = null;

    public function __construct(
        Model $parent,
        Model $related,
        mixed $pivotBuilder,
        string|int|null $foreignKey = null,
        string|int|null $otherKey = null
    ) {
        parent::__construct(parent: $parent, related: $related);

        $this->pivotBuilder = $pivotBuilder;
        $this->foreignKey = $foreignKey;
        $this->otherKey = $otherKey;
    }

    public function setJoin(): mixed
    {
        if ($this->eagerLoading) {
            $pivotQuery = $this->pivotBuilder->whereIn((string)$this->foreignKey, (array) $this->eagerKeys)->get();
        } else {
            $pivotQuery = $this->pivotBuilder->where(
                $this->foreignKey,
                $this->parent->getData(field: $this->parent->getPrimaryKey())
            )->get();
        }


        $otherId = [];

        $this->pivotResult = $pivotQuery->resultArray();
        foreach ($this->pivotResult as $row) {
            $otherId[] = $row[$this->otherKey];
        }

        $otherId = array_unique(array: $otherId);

        return !empty($otherId)
            ? $this->related->whereIn($this->related->getPrimaryKey(), $otherId)
            : $this->related->whereIn($this->related->getPrimaryKey(), []);
    }

    public function match(Model $parent): array
    {
        $return = [];

        foreach ($this->eagerResults as $row) {
            foreach ($this->pivotResult as $pivotRow) {
                if (
                    $parent->getData(field: $parent->getPrimaryKey()) == $pivotRow[$this->foreignKey] &&
                    $row->getData($row->getPrimaryKey()) == $pivotRow[$this->otherKey]
                ) {
                    $return[] = $row;
                    break;
                }
            }
        }

        return $return;
    }

    public function getResults(): mixed
    {
        if (empty($this->join)) {
            $this->join = $this->setJoin();
        }

        return $this->join->get();
    }
}
