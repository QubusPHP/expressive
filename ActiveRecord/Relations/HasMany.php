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

use Qubus\Expressive\ActiveRecord\Model;

class HasMany extends Relation
{
    protected int|string|null $foreignKey = null;

    public function __construct(Model $parent, Model $related, string|int|null $foreignKey = null)
    {
        parent::__construct($parent, $related);

        $this->foreignKey = $foreignKey;
    }

    public function setJoin()
    {
        if ($this->eagerLoading) {
            return $this->related->whereIn((string) $this->foreignKey, (array) $this->eagerKeys);
        } else {
            return $this->related->where(
                $this->foreignKey,
                $this->parent->getData(field: $this->parent->getPrimaryKey())
            );
        }
    }

    public function match(Model $parent): array
    {
        $return = [];

        foreach ($this->eagerResults as $row) {
            if ($row->{$this->foreignKey} === $parent->getData(field: $parent->getPrimaryKey())) {
                $return[] = $row;
            }
        }

        return $return;
    }

    public function getResults()
    {
        if (empty($this->join)) {
            $this->join = $this->setJoin();
        }

        return $this->join->get();
    }
}
