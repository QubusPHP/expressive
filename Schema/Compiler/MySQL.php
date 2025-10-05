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

namespace Qubus\Expressive\Schema\Compiler;

use Qubus\Expressive\Schema\AlterTable;
use Qubus\Expressive\Schema\BaseColumn;
use Qubus\Expressive\Schema\Compiler;
use Qubus\Exception\Exception;

class MySQL extends Compiler
{
    protected string $wrapper = '`%s`';

    protected function handleTypeInteger(BaseColumn $column): string
    {
        return match ($column->get(name: 'size', default: 'normal')) {
            'tiny' => 'TINYINT',
            'small' => 'SMALLINT',
            'medium' => 'MEDIUMINT',
            'big' => 'BIGINT',
            default => 'INT',
        };
    }

    protected function handleTypeDecimal(BaseColumn $column): string
    {
        if (null !== $l = $column->get('length')) {
            if (null === $p = $column->get('precision')) {
                return 'DECIMAL(' . $this->value($l) . ')';
            }
            return 'DECIMAL(' . $this->value($l) . ', ' . $this->value($p) . ')';
        }
        return 'DECIMAL';
    }

    protected function handleTypeBoolean(BaseColumn $column): string
    {
        return 'TINYINT(1)';
    }

    protected function handleTypeText(BaseColumn $column): string
    {
        return match ($column->get('size', 'normal')) {
            'tiny', 'small' => 'TINYTEXT',
            'medium' => 'MEDIUMTEXT',
            'big' => 'LONGTEXT',
            default => 'TEXT',
        };
    }

    protected function handleTypeBinary(BaseColumn $column): string
    {
        return match ($column->get('size', 'normal')) {
            'tiny', 'small' => 'TINYBLOB',
            'medium' => 'MEDIUMBLOB',
            'big' => 'LONGBLOB',
            default => 'BLOB',
        };
    }

    protected function handleDropPrimaryKey(AlterTable $table, $data): string
    {
        return 'ALTER TABLE ' . $this->wrap($table->getTableName()) . ' DROP PRIMARY KEY';
    }

    protected function handleDropUniqueKey(AlterTable $table, $data): string
    {
        return 'ALTER TABLE ' . $this->wrap($table->getTableName()) . ' DROP INDEX ' . $this->wrap($data);
    }

    protected function handleDropIndex(AlterTable $table, $data): string
    {
        return 'ALTER TABLE ' . $this->wrap($table->getTableName()) . ' DROP INDEX ' . $this->wrap($data);
    }

    protected function handleDropForeignKey(AlterTable $table, $data): string
    {
        return 'ALTER TABLE ' . $this->wrap($table->getTableName()) . ' DROP FOREIGN KEY ' . $this->wrap($data);
    }

    protected function handleSetDefaultValue(AlterTable $table, $data): string
    {
        return 'ALTER TABLE ' . $this->wrap($table->getTableName()) . ' ALTER '
        . $this->wrap($data['column']) . ' SET DEFAULT ' . $this->value($data['value']);
    }

    protected function handleDropDefaultValue(AlterTable $table, $data): string
    {
        return 'ALTER TABLE ' . $this->wrap($table->getTableName()) . ' ALTER ' . $this->wrap($data) . ' DROP DEFAULT';
    }

    /**
     * @throws Exception
     */
    protected function handleRenameColumn(AlterTable $table, $data): string
    {
        $tableName = $table->getTableName();
        $columnName = $data['from'];
        /** @var BaseColumn $column */
        $column = $data['column'];
        $newName = $column->getName();
        $columns = $this->connection->getSchema()->getColumns($tableName, false, false);
        $columnType = isset($columns[$columnName]) ? $columns[$columnName]['type'] : 'integer';

        return 'ALTER TABLE ' . $this->wrap($tableName) . ' CHANGE ' . $this->wrap($columnName)
        . ' ' . $this->wrap($newName) . ' ' . $columnType;
    }
}
