<?php

declare(strict_types=1);

namespace Qubus\Expressive\Migration\Seeder;

use Exception;
use Psr\Container\ContainerExceptionInterface;
use Psr\Container\NotFoundExceptionInterface;
use Qubus\Expressive\Migration\Seeder\Attribute\DependsOn;
use ReflectionClass;
use ReflectionException;

final class SeederTransaction
{
    public function __construct(private SeederContext $context)
    {
    }

    /**
     * @throws Exception
     */
    public function run(string $seederClass): void
    {
        $this->context->db->transactional(function () use ($seederClass) {
            $this->runWithDependencies($seederClass);
        });
    }

    /**
     * @throws ContainerExceptionInterface
     * @throws NotFoundExceptionInterface
     * @throws ReflectionException
     */
    private function runWithDependencies(string $class): void
    {
        $reflection = new ReflectionClass($class);

        foreach ($reflection->getAttributes(DependsOn::class) as $attribute) {
            $dependency = $attribute->newInstance()->seeder;
            $this->runWithDependencies($dependency);
        }

        $seeder = $this->context->resolve($class);
        $seeder->run($this->context);
    }
}
