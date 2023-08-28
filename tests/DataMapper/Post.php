<?php

declare(strict_types=1);

namespace Qubus\Tests\Expressive\DataMapper;

use Qubus\Expressive\DataMapper\Entity;
use Qubus\Expressive\DataMapper\Property;
use Qubus\Expressive\DataMapper\SerializableEntity;

#[Entity('qub_post')]
class Post extends SerializableEntity
{
    #[Property('post_id')]
    public int|string $id;

    #[Property('post_author')]
    public int $author;

    #[Property('post_title')]
    public string $title;

    #[Property('post_slug')]
    public string $slug;

    #[Property('post_content')]
    public string $content;

}
