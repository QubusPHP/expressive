<?php

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
