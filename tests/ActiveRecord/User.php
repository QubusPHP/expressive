<?php

declare(strict_types=1);

namespace Qubus\Tests\Expressive\ActiveRecord;

use Qubus\Expressive\ActiveRecord\Model;

class User extends Model
{
    protected string $primaryKey = 'user_id';

    protected ?string $tablePrefix = 'qub_';

    protected ?string $tableName = 'user';

    public function posts()
    {
        return $this->hasMany(related: Post::class, foreignKey: 'post_author');
    }
}
