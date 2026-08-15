<?php

declare(strict_types=1);

namespace Qubus\Tests\Expressive\ActiveRecord;

use Qubus\Expressive\ActiveRecord\Model;

class User extends Model
{
    protected bool $incrementing = false;

    protected string $primaryKey = 'user_id';

    protected ?string $tableName = 'users';
}
