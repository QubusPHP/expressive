# DataMapper

`PdoDataMapper` maps database rows to typed PHP entities while keeping persistence methods out of those entities. It is
a useful fit for domain-oriented code, services that inject persistence boundaries, and applications where the same
entity should not depend on a particular storage workflow.

Use [Active Record](active-record.md) when the convenience of calling `save()` and `delete()` on records is more
important than persistence independence.

## How the mapping works

The mapper uses PHP attributes and reflection to build a property-to-column map:

```text
Entity property       Mapper metadata       Database column
$user->login          #[Property('username')]    username
```

Queries and ordering use entity property names. Hydration arrays use database column names. Returned entities expose
their typed PHP properties.

## Defining an entity

An entity must:

- extend `SerializableEntity`;
- declare one `#[Entity]` attribute with a non-empty table name;
- declare every mapped property as public and non-static;
- add exactly one `#[Property]` mapping to every reflected property;
- define a mapped property named `id`, even when its database column has another name.

```php
<?php

declare(strict_types=1);

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
    public string $login;

    #[Property('first_name')]
    public string $firstName;

    #[Property('last_name')]
    public string $lastName;

    #[Property('email')]
    public string $email;
}
```

The PHP type of each property must accept the value returned by the selected PDO driver. IDs are commonly declared
`int|string` because an application-generated UUID or ULID is a string and PDO's `lastInsertId()` also returns a
string.

Mapper construction validates this metadata early. Invalid entity classes, missing attributes, non-public or static
mapped properties, an empty table, and a missing `id` property raise `DataMapperException`.

## Creating a mapper

Create a connection, then bind one mapper instance to one entity class:

```php
<?php

use App\Entity\User;
use Qubus\Expressive\Connection\DriverConnection;
use Qubus\Expressive\DataMapper\PdoDataMapper;

$connection = DriverConnection::make(
    'mysql://app_user:password@localhost:3306/app?charset=utf8mb4'
);

$users = new PdoDataMapper($connection, User::class);
```

The mapper keeps its connection in the public readonly `$connection` property. Different mapper instances may safely
use different connections.

## Reading entities

### Find one by ID

`findOne()` uses the database column mapped from the entity's `id` property. It returns an entity or `null`:

```php
$user = $users->findOne('01K6TYX0XPE1KVWHC1QNA1NYMP');

if ($user !== null) {
    echo $user->login;
}
```

The ID is bound as a PDO parameter.

### Find a page of entities

`findAll()` returns an array keyed by entity ID:

```php
$page = $users->findAll(
    orderBy: 'login',
    options: [
        'direction' => 'ASC',
        'limit' => 25,
        'offset' => 0,
    ],
);

foreach ($page as $id => $user) {
    echo $id . ': ' . $user->login;
}
```

Defaults are significant:

- `orderBy` defaults to the entity property `id`;
- `direction` defaults to `ASC`;
- `limit` defaults to `10`;
- `offset` defaults to `0`.

`limit: 0` returns no rows. Limit and offset must be non-negative integers. Direction is case-insensitive but must be
`ASC` or `DESC`.

Because results are keyed by ID, duplicate IDs overwrite earlier entries during hydration. A correctly keyed table
should not produce duplicates.

### Find entities by one property

`findAllBy()` supports an equality filter on one mapped entity property:

```php
$matches = $users->findAllBy(
    column: 'login',
    value: 'person',
    orderBy: 'lastName',
    options: [
        'direction' => 'DESC',
        'limit' => 10,
        'offset' => 0,
    ],
);
```

`column` and `orderBy` are PHP property names (`login`, `lastName`), not database names (`username`, `last_name`). The
filter value is bound. Unknown properties raise `DataMapperException`, which also prevents arbitrary input from being
treated as a SQL identifier.

`findAllBy()` supports equality only. Use the QueryBuilder escape hatch for multiple predicates, ranges, `LIKE`, joins,
or other query shapes.

## Hydrating existing rows

`hydrate()` converts an in-memory list of associative rows to entities. Input keys are database column names:

```php
$entities = $users->hydrate([
    [
        'user_id' => '01K6TYX0XPE1KVWHC1QNA1NYMP',
        'username' => 'person',
        'first_name' => 'Pat',
        'last_name' => 'Developer',
        'email' => 'person@example.com',
    ],
]);

$user = $entities['01K6TYX0XPE1KVWHC1QNA1NYMP'];
```

Every mapped database column must be present in every row, including nullable columns. Use a `null` value for a nullable
property rather than omitting its key. A missing column raises `DataMapperException` instead of returning a partially
initialized entity.

Like database reads, `hydrate()` returns an array keyed by the mapped ID value.

## Creating entities

### Application-generated IDs

Initialize every mapped property, then pass the entity to `create()`:

```php
$user = new User();
$user->id = '01KDATAMAPPER00000000000001';
$user->login = 'person';
$user->firstName = 'Pat';
$user->lastName = 'Developer';
$user->email = 'person@example.com';

$created = $users->create($user);
```

An initialized `id` is included in the insert and preserved. `create()` returns the same entity instance; it does not
clone it.

### Auto-incrementing IDs

Leave the typed `id` property uninitialized when the database generates it:

```php
$user = new User();
$user->login = 'person';
$user->firstName = 'Pat';
$user->lastName = 'Developer';
$user->email = 'person@example.com';

$users->create($user);

echo $user->id; // Assigned from PDO::lastInsertId().
```

Only an uninitialized `id` is omitted. Every other mapped property must be initialized before insertion, even when the
database column has a default. A property initialized to `null` is included and must have a nullable PHP type.

Pass an instance of the entity class used to construct the mapper. The method accepts the common
`SerializableEntity` base type, but its mapping metadata belongs to the configured class.

## Updating entities

Change public properties and pass the entity to `update()`:

```php
$user = $users->findOne($id);

if ($user !== null) {
    $user->email = 'new-address@example.com';
    $updated = $users->update($user);
}
```

The mapper:

- writes every mapped property except `id`;
- binds `id` in the `WHERE` clause;
- requires every mapped property, including `id`, to be initialized;
- returns the same entity instance.

There is no dirty tracking, optimistic-lock column, lifecycle callback, or affected-row assertion. If those rules are
part of the domain, enforce them in a repository or service around the mapper.

## Deleting entities

Delete by mapped ID:

```php
$users->delete($user->id);
```

`delete()` binds the ID and returns `void`. It does not mutate the entity or report whether a row existed.

## Transactions

The mapper does not start transactions around individual writes. Group related work with the connection's
`transactional()` method:

```php
$connection->transactional(function () use ($users, $user): void {
    $users->create($user);

    // Additional mapper or QueryBuilder writes participate in the same connection transaction.
});
```

Any `Throwable` escaping the callback triggers a rollback and is rethrown. See [Connections](connections.md) for nested
transaction behavior.

## QueryBuilder escape hatch

`queryBuilder()` returns a builder already pointed at the entity table:

```php
$rows = $users->queryBuilder()
    ->select(['user_id', 'username', 'email'])
    ->whereLike('username', 'pat%')
    ->orderBy('username', 'ASC')
    ->find();
```

Use database column names with this builder because it bypasses Data Mapper metadata. Its rows are QueryBuilder row
objects, not mapped entities. If a custom query selects all mapped columns, convert its associative rows with
`hydrate()` when entity objects are needed.

`getPdo()` is also available for PDO-specific operations:

```php
$pdo = $users->getPdo();
```

Prefer mapper methods, QueryBuilder, or parameterized PDO statements over interpolating values into SQL.

## Entity serialization

`SerializableEntity` implements `Stringable`. Casting an entity to a string JSON-encodes its public properties and
throws `JsonException` if encoding fails:

```php
echo (string) $user;
```

This is deliberately small: the base entity does not implement `JsonSerializable`, hide properties, rename fields, or
recursively load relationships. Add application-specific serialization in a DTO, presenter, or entity subclass when a
public API needs a stable representation.

## Errors and validation boundaries

`DataMapperException` is used for mapper-specific validation, including:

- invalid entity or property metadata;
- an unknown property used as a filter or sort identifier;
- invalid sort direction, limit, or offset;
- a missing column during hydration;
- an uninitialized property required for a write.

PDO statement and constraint failures are not translated into domain exceptions. Catch them at the persistence
boundary if the application needs to turn them into validation, conflict, or retry behavior.

The mapper assigns database values directly to typed properties. Type mismatches can therefore raise `TypeError`.
Choose property types that represent both the domain value and the values produced by the PDO driver, or normalize data
in a repository layer.

## Data Mapper and Active Record compared

| Concern                  | Data Mapper                        | Active Record                               |
|--------------------------|------------------------------------|---------------------------------------------|
| Persistence methods      | Separate mapper                    | On models and rows                          |
| Entity/record properties | Declared, public, typed            | Dynamic attributes in an internal array     |
| Mapping                  | PHP attributes per property        | Table and key properties on the model       |
| Query result             | Entity or ID-keyed entity array    | `Row` or iterable `Result`                  |
| Relationships            | Implement in repositories/services | Built-in relation objects and eager loading |
| Multiple connections     | One connection per mapper          | One shared model connection                 |
| Best fit                 | Domain and service boundaries      | Direct CRUD and record-oriented code        |

Neither abstraction replaces database constraints, transactions, authorization, or validation. Treat those as
explicit application and schema responsibilities.
