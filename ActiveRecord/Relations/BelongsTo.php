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

class BelongsTo extends Relation
{
    protected int|string|null $foreignKey = null;

    public function __construct(Model $parent, Model $related, string|int|null $foreignKey = null)
    {
        parent::__construct(parent: $parent, related: $related);

        $this->foreignKey = $foreignKey;
    }

    public function setJoin()
    {
        if ($this->eagerLoading) {
            return $this->related->whereIn((string)$this->related->getPrimaryKey(), (array) $this->eagerKeys);
        } else {
            return $this->related->where(
                $this->related->getPrimaryKey(),
                $this->parent->getData(field: $this->foreignKey)
            );
        }
    }

    public function match(Model $parent)
    {
        foreach ($this->eagerResults as $row) {
            if ($parent->{$this->foreignKey} === $row->getData($row->getPrimaryKey())) {
                return $row;
            }
        }
    }

    public function getResults()
    {
        if (empty($this->join)) {
            $this->join = $this->setJoin();
        }

        return $this->join->first();
    }
}
