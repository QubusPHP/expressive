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

use Attribute;

#[Attribute(Attribute::TARGET_CLASS)]
class Entity
{
    private ?string $table = null;

    public function __construct(?string $table = null)
    {
        $this->table = $table;
    }

    public function getTable(): string
    {
        return $this->table;
    }

    public function setTable(string $table): void
    {
        $this->table = $table;
    }
}
