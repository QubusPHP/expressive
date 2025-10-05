<?php

declare(strict_types=1);

namespace Qubus\Expressive;

final class Structure
{
    private string $primaryKey = 'id';

    private string $foreignKey = '%s_id';

    /**
     * Structure constructor
     *
     * @param string $primaryKey
     * @param string $foreignKey
     */
    public function __construct(string $primaryKey = 'id', string $foreignKey = '%s_id')
    {
        if ($foreignKey === null) {
            $foreignKey = $primaryKey;
        }
        $this->primaryKey = $primaryKey;
        $this->foreignKey = $foreignKey;
    }

    /**
     * @param string $table
     *
     * @return string
     */
    public function getPrimaryKey(string $table): string
    {
        return $this->key($this->primaryKey, $table);
    }

    /**
     * @param string $table
     *
     * @return string
     */
    public function getForeignKey(string $table): string
    {
        return $this->key($this->foreignKey, $table);
    }

    /**
     * @param string $key
     * @param string $table
     *
     * @return string
     */
    private function key(string $key, string $table): string
    {
        return sprintf($key, $table);
    }
}
