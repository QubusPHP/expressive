<?php

declare(strict_types=1);

namespace Qubus\Tests\Expressive\ActiveRecord;

use Qubus\Expressive\ActiveRecord\Model;

class Post extends Model
{
    protected string $primaryKey = 'post_id';

    protected ?string $tablePrefix = 'qub_';

    protected ?string $tableName = 'post';

    public function owner()
    {
        return $this->belongsTo(related: User::class, foreignKey: 'post_author');
    }
}
