<?php

declare(strict_types=1);

namespace Qubus\Expressive\Migration\Seeder;

use Faker\Factory;
use Faker\Generator;
use Psr\Container\ContainerExceptionInterface;
use Psr\Container\NotFoundExceptionInterface;

abstract class BaseSeeder implements Seeder
{
    protected Generator $faker;

    final public function __construct()
    {
        $this->faker = Factory::create();
    }

    /**
     * Set a deterministic Faker seed
     */
    public function withFakerSeed(int $seed): static
    {
        $this->faker->seed($seed);
        return $this;
    }

    /**
     * Call another seeder
     *
     * @throws NotFoundExceptionInterface
     * @throws ContainerExceptionInterface
     */
    protected function call(
        string|Seeder $seeder,
        SeederContext $context
    ): void {
        $instance = is_string($seeder)
        ? $context->resolve($seeder)
        : $seeder;

        $instance->run($context);
    }
}
