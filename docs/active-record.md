# Active Record

Active Record models combine row data with persistence behavior. Use DataMapper instead when entities should remain
independent of persistence.

## Defining a model

```php
<?php

namespace App\Model;

use Qubus\Expressive\ActiveRecord\Model;

final class User extends Model
{
    protected ?string $tableName = 'users';

    protected string $primaryKey = 'user_id';

    // Use false for UUIDs, ULIDs, and other application-generated IDs.
    protected bool $incrementing = false;

    protected array $fillable = ['user_id', 'username', 'email'];
}
```

A missing connection or table name now produces a `LogicException` with a clear configuration message.

Configure the connection before querying:

```php
User::connection($connection);
```

## Querying

Protected model methods are exposed through the model's static forwarding API:

```php
$users = User::all();
$user = User::find('01KUSER000000000000000001');
$first = User::where('active', true)->first();
```

Collection queries return `Result`; a single-row query returns `Row` or `null`. QueryBuilder filters are forwarded by
the model, so methods such as `where()`, `whereIn()`, and `orderBy()` can be chained.

## Creating

`create()` instantiates and returns the called model subclass:

```php
$user = User::create([
    'user_id' => '01KUSER000000000000000001',
    'username' => 'person',
    'email' => 'person@example.com',
]);
```

For non-incrementing models, the primary key must be present. Failed persistence returns `false` instead of returning an
unsaved model as if creation succeeded.

## Updating and deleting rows

Rows returned by `find()` and `first()` can be changed and persisted directly:

```php
$user = User::find('01KUSER000000000000000001');

if ($user !== null) {
    $user->email = 'new-address@example.com';
    $affected = $user->save();

    $user->delete();
}
```

`save()` and `delete()` are public persistence operations. Updates build the primary-key filter before executing SQL,
preventing an unfiltered update. Read-only models throw `ReadOnlyException` for persistence operations.

## Mass assignment

Use `$fillable` to allowlist mass-assignable fields or `$guarded` to denylist fields. When neither list is populated,
all supplied fields are assigned. Direct values such as `0`, `false`, and empty strings remain present and are reported
correctly by `isset()`.

