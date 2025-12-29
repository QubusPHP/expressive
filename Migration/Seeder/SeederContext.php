<?php

declare(strict_types=1);

namespace Qubus\Expressive\Migration\Seeder;

use Psr\Container\ContainerExceptionInterface;
use Psr\Container\ContainerInterface;
use Psr\Container\NotFoundExceptionInterface;
use Qubus\Expressive\Database;

final readonly class SeederContext
{
    public function __construct(
        public Database $db,
        public ContainerInterface $container,
        public string $environment,
        public bool $isProduction,
    ) {
    }

    /**
     * @throws ContainerExceptionInterface
     * @throws NotFoundExceptionInterface
     */
    public function resolve(string $class): Seeder
    {
        return $this->container->get($class);
    }
}
