<?php

declare(strict_types=1);

namespace Qubus\Expressive\Helpers;

use Faker\Generator;
use PDO;

use function is_bool;
use function is_int;
use function Qubus\Support\Helpers\is_null__;

function detect_type(mixed $value): int
{
    return match (true) {
        is_int($value)      => PDO::PARAM_INT,
        is_bool($value)     => PDO::PARAM_BOOL,
        is_null__($value)   => PDO::PARAM_NULL,
        default             => PDO::PARAM_STR,
    };
}

/**
 * Generate fake data.
 *
 * @return Generator
 */
function fake(): Generator
{
    return \Faker\Factory::create();
}
