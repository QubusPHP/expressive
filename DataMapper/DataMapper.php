<?php

/**
 * Qubus\Expressive
 *
 * @link       https://github.com/QubusPHP/expressive
 * @copyright  2022
 * @author     Joshua Parker <joshua@joshuaparker.dev>
 * @license    https://opensource.org/licenses/mit-license.php MIT License
 */

declare(strict_types=1);

namespace Qubus\Expressive\DataMapper;

interface DataMapper
{
    public function findAll(string $orderBy = '', array $options = []): array;

    public function findOne(int|string $id): ?SerializableEntity;

    public function create(SerializableEntity $entity): SerializableEntity;

    public function update(SerializableEntity $entity): SerializableEntity;

    public function delete(int|string $id): void;
}
