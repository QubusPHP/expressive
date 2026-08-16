<?php

declare(strict_types=1);

namespace Qubus\Expressive\DataMapper;

interface DataMapper
{
    /**
     * @param array{direction?: string, limit?: int, offset?: int} $options
     * @return array<int|string, SerializableEntity>
     */
    public function findAll(string $orderBy = '', array $options = []): array;

    public function findOne(int|string $id): ?SerializableEntity;

    public function create(SerializableEntity $entity): SerializableEntity;

    public function update(SerializableEntity $entity): SerializableEntity;

    public function delete(int|string $id): void;
}
