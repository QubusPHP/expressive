<?php

declare(strict_types=1);

namespace Qubus\Tests\Expressive\DataMapper;

use Qubus\Expressive\DataMapper\Entity;
use Qubus\Expressive\DataMapper\Property;
use Qubus\Expressive\DataMapper\SerializableEntity;

#[Entity('users')]
class User extends SerializableEntity
{
    #[Property('user_id')]
    public int|string $id;

    #[Property('username')]
    public int|string $login;

    #[Property('first_name')]
    public string $fname;

    #[Property('last_name')]
    public string $lname;

    #[Property('email')]
    public string $email;
}
