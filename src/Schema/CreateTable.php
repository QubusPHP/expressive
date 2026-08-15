<?php

/* ===========================================================================
 * Copyright 2013-2015 Marius Sarca
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ============================================================================ */

declare(strict_types=1);

namespace Qubus\Expressive\Schema;

use function implode;
use function is_array;

class CreateTable
{
    /** @var CreateColumn[] $columns */
    protected array $columns = [];

    /** @var array{name: string, columns: list<string>}|null */
    protected ?array $primaryKey = null;

    /** @var array<string, list<string>> $uniqueKeys */
    protected array $uniqueKeys = [];

    /** @var array<string, list<string>> $indexes */
    protected array $indexes = [];

    /** @var array<string, ForeignKey> $foreignKeys */
    protected array $foreignKeys = [];

    protected string $table;

    /** @var string|null */
    protected ?string $engine = null;

    /** @var ?BaseColumn $autoincrement */
    protected ?BaseColumn $autoincrement = null;

    public function __construct(string $table)
    {
        $this->table = $table;
    }

    protected function addColumn(string $name, string $type): CreateColumn
    {
        $column = new CreateColumn(table: $this, name: $name, type: $type);
        $this->columns[$name] = $column;
        return $column;
    }

    public function getTableName(): string
    {
        return $this->table;
    }

    /**
     * @return CreateColumn[]
     */
    public function getColumns(): array
    {
        return $this->columns;
    }

    /**
     * @return array{name: string, columns: list<string>}|null
     */
    public function getPrimaryKey(): ?array
    {
        return $this->primaryKey;
    }

    /**
     * @return  array
     */
    public function getUniqueKeys(): array
    {
        return $this->uniqueKeys;
    }

    /**
     * @return  array
     */
    public function getIndexes(): array
    {
        return $this->indexes;
    }

    /**
     * @return  array
     */
    public function getForeignKeys(): array
    {
        return $this->foreignKeys;
    }

    /**
     * @return  string|null
     */
    public function getEngine(): ?string
    {
        return $this->engine;
    }

    /**
     * @return BaseColumn|null
     */
    public function getAutoincrement(): ?BaseColumn
    {
        return $this->autoincrement;
    }

    /**
     * @return $this
     */
    public function engine(string $name): self
    {
        $this->engine = $name;
        return $this;
    }

    /**
     * @param string|string[] $columns
     * @return $this
     */
    public function primary(array|string $columns, ?string $name = null): self
    {
        $columns = is_array(value: $columns) ? array_values($columns) : [$columns];

        if ($name === null) {
            $name = $this->table . '_pk_' . implode(separator: '_', array: $columns);
        }

        $this->primaryKey = [
            'name'    => $name,
            'columns' => $columns,
        ];

        return $this;
    }

    /**
     * @param string|string[] $columns
     * @return $this
     */
    public function unique(array|string $columns, ?string $name = null): self
    {
        $columns = is_array(value: $columns) ? array_values($columns) : [$columns];

        if ($name === null) {
            $name = $this->table . '_uk_' . implode(separator: '_', array: $columns);
        }

        $this->uniqueKeys[$name] = $columns;

        return $this;
    }

    /**
     * @param string|string[] $columns
     * @return $this
     */
    public function index(array|string $columns, ?string $name = null): self
    {
        $columns = is_array(value: $columns) ? array_values($columns) : [$columns];

        if ($name === null) {
            $name = $this->table . '_ik_' . implode(separator: '_', array: $columns);
        }

        $this->indexes[$name] = $columns;

        return $this;
    }

    /**
     * @param string|string[] $columns
     */
    public function foreign(array|string $columns, ?string $name = null): ForeignKey
    {
        $columns = is_array(value: $columns) ? array_values($columns) : [$columns];

        if ($name === null) {
            $name = $this->table . '_fk_' . implode(separator: '_', array: $columns);
        }

        return $this->foreignKeys[$name] = new ForeignKey(columns: $columns);
    }

    /**
     * @return $this
     */
    public function autoincrement(CreateColumn $column, ?string $name = null): self
    {
        if ($column->getType() !== 'integer') {
            return $this;
        }

        $this->autoincrement = $column->set('autoincrement', true);
        return $this->primary($column->getName(), $name);
    }

    public function integer(string $name): CreateColumn
    {
        return $this->addColumn(name: $name, type: 'integer');
    }

    public function float(string $name): CreateColumn
    {
        return $this->addColumn(name: $name, type: 'float');
    }

    public function double(string $name): CreateColumn
    {
        return $this->addColumn(name: $name, type: 'double');
    }

    public function decimal(string $name, ?int $length = null, ?int $precision = null): CreateColumn
    {
        return $this->addColumn(
            name: $name,
            type: 'decimal'
        )->length(value: $length)->set(name: 'precision', value: $precision);
    }

    public function boolean(string $name): CreateColumn
    {
        return $this->addColumn(name: $name, type: 'boolean');
    }

    public function binary(string $name): CreateColumn
    {
        return $this->addColumn(name: $name, type: 'binary');
    }

    public function string(string $name, int $length = 255): CreateColumn
    {
        return $this->addColumn(name: $name, type: 'string')->length(value: $length);
    }

    public function fixed(string $name, int $length = 255): CreateColumn
    {
        return $this->addColumn(name: $name, type: 'fixed')->length(value: $length);
    }

    public function text(string $name): CreateColumn
    {
        return $this->addColumn(name: $name, type: 'text');
    }

    public function time(string $name): CreateColumn
    {
        return $this->addColumn(name: $name, type: 'time');
    }

    public function timestamp(string $name): CreateColumn
    {
        return $this->addColumn(name: $name, type: 'timestamp');
    }

    public function date(string $name): CreateColumn
    {
        return $this->addColumn(name: $name, type: 'date');
    }

    public function dateTime(string $name): CreateColumn
    {
        return $this->addColumn(name: $name, type: 'dateTime');
    }

    /**
     * @return $this
     */
    public function softDelete(string $column = 'deleted_at'): self
    {
        $this->dateTime(name: $column);
        return $this;
    }

    /**
     * @return $this
     */
    public function timestamps(string $createColumn = 'created_at', string $updateColumn = 'updated_at'): self
    {
        $this->dateTime(name: $createColumn)->notNull();
        $this->dateTime(name: $updateColumn);
        return $this;
    }
}
