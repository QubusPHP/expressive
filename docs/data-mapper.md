# DataMapper

`PdoDataMapper` maps database rows to typed entity properties without requiring entities to contain persistence logic.

## Defining an entity

An entity must extend `SerializableEntity`, declare an `Entity` table attribute, and expose public, non-static mapped
properties. Every reflected property must have a `Property` attribute, and an `id` property is required.

```php
<?php

namespace App\Entity;

use Qubus\Expressive\DataMapper\Entity;
use Qubus\Expressive\DataMapper\Property;
use Qubus\Expressive\DataMapper\SerializableEntity;

#[Entity('users')]
final class User extends SerializableEntity
{
    #[Property('user_id')]
    public int|string $id;

    #[Property('username')]
    public string $username;

    #[Property('email')]
    public string $email;
}
```

Invalid entity metadata produces `DataMapperException` during mapper construction instead of failing later while
hydrating or executing SQL.

## Creating a mapper

```php
<?php

use App\Entity\User;
use Qubus\Expressive\DataMapper\PdoDataMapper;

$users = new PdoDataMapper($connection, User::class);
```

## Finding entities

```php
$user = $users->findOne(42); // User|null

$all = $users->findAll(
    orderBy: 'username',
    options: [
        'direction' => 'ASC',
        'limit' => 25,
        'offset' => 0,
    ]
);

$matches = $users->findAllBy(
    column: 'username',
    value: 'person',
    orderBy: 'username',
    options: ['direction' => 'DESC']
);
```

`orderBy` and `column` use entity property names, not database column names. Unknown properties throw
`DataMapperException`. Direction is limited to `ASC` or `DESC`; limit and offset must be non-negative. `findAll()` and
`findAllBy()` default to a limit of 10.

Returned collections are keyed by entity ID.

## Creating entities

Client-generated IDs such as UUIDs and ULIDs are preserved:

```php
$user = new User();
$user->id = '01KDATAMAPPER00000000000001';
$user->username = 'person';
$user->email = 'person@example.com';

$users->create($user);
```

For an auto-incrementing ID, leave the typed `id` property uninitialized. After insertion, the mapper assigns PDO's
`lastInsertId()` to it. All other mapped properties must be initialized before insertion.

## Updating and deleting entities

```php
$user->email = 'new-address@example.com';
$users->update($user);

$users->delete($user->id);
```

Updates bind every mapped property and restrict the statement by the mapped ID. Deletes also use a bound ID parameter.

## Hydrating existing data

`hydrate()` accepts a list of rows keyed by database column names:

```php
$entities = $users->hydrate([
    [
        'user_id' => 42,
        'username' => 'person',
        'email' => 'person@example.com',
    ],
]);
```

Missing mapped columns throw `DataMapperException`, preventing partially initialized entities from being returned.

