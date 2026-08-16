# Active Record

Expressive Active Record combines a table row and its persistence behavior. It is a convenient fit for CRUD-oriented
applications where working with records as objects is more useful than keeping the domain model independent of the
database. Use [DataMapper](data-mapper.md) when entities should not know how they are stored.

This guide assumes that Expressive is installed and a `Connection` has already been created. See
[Connections](connections.md) for DSN and driver configuration.

## How the API is structured

Three types take part in normal Active Record work:

| Type     | Represents                                    | Common operations                                  |
|----------|-----------------------------------------------|----------------------------------------------------|
| `Model`  | Table configuration and a new, unsaved record | `create()`, `save()`, query construction           |
| `Row`    | One record read from the database             | attribute access, `save()`, `delete()`             |
| `Result` | A collection of `Row` objects                 | iteration, `count()`, eager loading, serialization |

The static-looking API is forwarded to a new instance of the called model. Query-builder methods are then forwarded by
that instance to an internal [QueryBuilder](query-builder.md).

```php
$published = Post::where('status', 'published')
    ->orderBy('published_at', 'DESC')
    ->limit(20)
    ->get(); // Result<Post rows>
```

## Defining a model

Extend `Qubus\Expressive\ActiveRecord\Model` and configure the table:

```php
<?php

declare(strict_types=1);

namespace App\Model;

use Qubus\Expressive\ActiveRecord\Model;

final class Post extends Model
{
    protected ?string $tableName = 'posts';

    protected ?string $tablePrefix = null;

    protected string $primaryKey = 'post_id';

    // Set false for UUIDs, ULIDs, and other application-generated IDs.
    protected bool $incrementing = false;
}
```

The model configuration properties are:

- `$tableName` is the table without its optional prefix. It is required.
- `$tablePrefix` is prepended to the table name by the query builder. It defaults to `null`.
- `$primaryKey` is the primary-key column. It defaults to `id`.
- `$incrementing` tells `save()` whether the database generates the primary key. It defaults to `true`.
- `$fillable` is an allowlist for attributes accepted by the model.
- `$guarded` is a denylist for attributes rejected by the model.
- `$isReadOnly` prevents `create`, `update`, `save`, and `delete` operations when set to `true`.

A missing connection or table name raises `LogicException` when a query is attempted.

### Configure the connection

Register a connection before using any model:

```php
<?php

use App\Model\Post;
use Qubus\Expressive\Connection\DriverConnection;

$connection = DriverConnection::make(
    'mysql://app_user:password@localhost:3306/app?charset=utf8mb4'
);

Post::connection($connection);
```

The connection is stored by the base `Model` and is shared by Active Record subclasses in the current PHP process.
Calling `connection()` again replaces it for those models. If an application must query multiple databases at the same
time, keep separate query builders or use Data Mapper instances bound to their own connections.

## Reading records

### Retrieve all records

`all()` returns a `Result`, including an empty `Result` when there are no matches:

```php
$posts = Post::all();

foreach ($posts as $post) {
    echo $post->title;
}

echo count($posts);
```

### Find by primary key

One ID returns `Row|null`:

```php
$post = Post::find('01K7A51JXJEFBBT89K10CA13H1');

if ($post !== null) {
    echo $post->title;
}
```

An array or multiple ID arguments returns a `Result`:

```php
$posts = Post::find([
    '01K74SR0BRE4093D40SR087PPR',
    '01K7A51H9EE1PSAQ3TW8897XTG',
]);

$samePosts = Post::find(
    '01K74SR0BRE4093D40SR087PPR',
    '01K7A51H9EE1PSAQ3TW8897XTG',
);
```

### Build filtered queries

Most QueryBuilder filters can be used through the model proxy. Call `get()` to execute the accumulated query and
hydrate a `Result`:

```php
$posts = Post::where('status', 'published')
    ->whereNotNull('published_at')
    ->whereLike('title', 'PHP%')
    ->orderBy('published_at', 'DESC')
    ->limit(20)
    ->get();
```

Other useful forwarded methods include `whereIn()`, `whereNot()`, `whereGt()`, `whereGte()`, `whereLt()`,
`whereLte()`, `whereNull()`, `whereNotNull()`, `groupBy()`, `offset()`, and `pagination()`.

Values are bound through PDO. Column names, sort columns, select expressions, and other SQL identifiers are not values;
only pass identifiers selected by trusted application code or an allowlist.

### Retrieve one filtered record

At present, Active Record's `first()` and `pluck()` methods build a fresh table query. They do not retain filters,
ordering, or other methods previously forwarded to the query builder. Use them directly only when an unfiltered result
is intended:

```php
$firstPost = Post::first();
$title = Post::pluck('title');
```

Do not use `Post::where(...)->first()` or `Post::orderBy(...)->pluck()` when the condition or ordering matters. Until
those terminals retain query state, use the underlying QueryBuilder for a filtered single row:

```php
$post = Post::dbalQuery()
    ->where('status', 'published')
    ->orderBy('published_at', 'DESC')
    ->findOne();

if ($post !== false) {
    echo $post->title;
}
```

The escape-hatch result above is a QueryBuilder row, not an Active Record `Row`.

### Select specific columns

`all()`, `get()`, and `first()` accept a column name, a comma-delimited string, or an array:

```php
$posts = Post::all(['post_id', 'title']);

$posts = Post::where('status', 'published')
    ->get(['post_id', 'title']);

$firstPost = Post::first(['post_id', 'title']);
```

`find()` does not accept a column selection. Include the primary key whenever a partial row may later be saved or used
to resolve a relationship.

### Aggregates

Aggregate methods execute immediately and retain forwarded query filters:

```php
$numberOfPosts = Post::count();
$highestViews = Post::max('view_count');
$lowestViews = Post::min('view_count');
$averageViews = Post::avg('view_count');

$publishedViews = Post::where('status', 'published')->sum('view_count');
```

`count()` uses the model primary key when no field is supplied. `avg()` is rounded to two decimal places.

## Creating records

### Instantiate and save

Assign attributes to a model and call `save()`:

```php
$post = new Post();
$post->post_id = '01KNEWPOST0000000000000001';
$post->title = 'A new title';
$post->content = 'More content to come.';

$result = $post->save();

if ($result !== false) {
    echo $post->post_id;
}
```

For a non-incrementing model, `save()` returns `false` if the primary key is absent or empty. For an incrementing model,
omit the key; after a successful insert, the value returned by PDO's `lastInsertId()` is assigned to the configured
primary-key attribute.

### Create in one call

`create()` constructs the called subclass, saves it, and returns the model. It returns `false` for an empty payload, a
missing application-generated primary key, or a persistence operation that reports failure.

```php
$post = Post::create([
    'post_id' => '01KNEWPOST0000000000000001',
    'title' => 'A new title',
    'content' => 'More content to come.',
]);

if ($post !== false) {
    echo $post->post_id;
}
```

Database exceptions are not converted to `false`; handle them at the application's transaction or persistence
boundary.

## Updating records

### Update a retrieved row

`Row` delegates attribute access and persistence to its underlying model:

```php
$post = Post::find('01K7A51JXJEFBBT89K10CA13H1');

if ($post !== null) {
    $post->title = 'A different title';
    $affected = $post->save();
}
```

The update is restricted by the model's primary key. `save()` sends all attributes currently held by the model, not
only changed attributes.

### Update matching rows

Use a forwarded filter for a bulk update:

```php
$affected = Post::where('status', 'draft')->update([
    'review_required' => true,
]);
```

Or supply an explicit condition as the second argument to the static proxy:

```php
$affected = Post::update(
    ['title' => 'A different title'],
    ['post_id' => '01K7A51JXJEFBBT89K10CA13H1'],
);
```

Always provide a deliberate condition for a bulk update.

## Deleting records

Delete a retrieved row:

```php
$post = Post::find('01K7A51JXJEFBBT89K10CA13H1');

if ($post !== null) {
    $affected = $post->delete();
}
```

Delete records matching a query:

```php
$affected = Post::where('status', 'archived')->delete();
```

Because `delete()` is a public instance method, do not call `Post::delete($id)` statically. Find the row first or build
a filtered delete as shown above. The underlying QueryBuilder refuses an unfiltered delete unless its explicit
`deleteAll` option is used; Active Record does not expose that option through `Model::delete()`.

## Transactions

Active Record does not start a transaction around an individual `save()`, `update()`, or `delete()`. Retain the
connection used during setup and group related work with `transactional()`:

```php
$connection->transactional(function (): void {
    $post = Post::create([
        'post_id' => '01KNEWPOST0000000000000001',
        'title' => 'A new title',
    ]);

    if ($post === false) {
        throw new \RuntimeException('The post could not be created.');
    }

    AuditEntry::create([
        'audit_id' => '01KNEWAUDIT000000000000001',
        'action' => 'post.created',
        'subject_id' => $post->post_id,
    ]);
});
```

Any `Throwable` escaping the callback rolls back the transaction and is rethrown. All models in the callback must use
that same connection for their writes to participate in the transaction.

## Mass-assignment protection

Use `$fillable` as an allowlist:

```php
final class Post extends Model
{
    protected ?string $tableName = 'posts';
    protected string $primaryKey = 'post_id';
    protected bool $incrementing = false;

    protected array $fillable = [
        'post_id',
        'title',
        'content',
        'status',
    ];
}
```

Or use `$guarded` as a denylist:

```php
protected array $guarded = ['is_admin', 'internal_notes'];
```

When both arrays are empty, every supplied attribute is accepted. If both are populated, an attribute must be in
`$fillable` and absent from `$guarded`.

In the current implementation these rules apply to constructor data, `setData()`, direct property assignment, and rows
hydrated from the database. A `$fillable` model must therefore include every column that the application expects to
read as well as write. Values such as `0`, `false`, and `''` are retained; `isset($model->field)` is false for a missing
field or a `null` value, following normal PHP `isset()` behavior.

## Query scopes

A scope packages a reusable query fragment on the model. Its method name starts with `scope`; callers omit that prefix.
The first parameter receives the model's fluent query proxy.

```php
<?php

namespace App\Model;

use Qubus\Expressive\ActiveRecord\Model;

final class Post extends Model
{
    protected ?string $tableName = 'posts';

    protected function scopePublished(Model $query): Model
    {
        return $query
            ->where('status', 'published')
            ->whereNotNull('published_at');
    }

    protected function scopeSearch(Model $query, string $term): Model
    {
        return $query
            ->where('status', 'published')
            ->whereLike('title', '%' . $term . '%');
    }
}
```

Execute scopes with `get()` or an aggregate terminal:

```php
$posts = Post::published()
    ->orderBy('published_at', 'DESC')
    ->get();

$matches = Post::search('Expressive')->get();
$count = Post::published()->count();
```

## Relationships

Relationship methods live on the parent model and return a relation object. Four relationship types are available:

| Method            | Property result             | Database shape                        |
|-------------------|-----------------------------|---------------------------------------|
| `hasOne()`        | one `Row` or an empty value | related table contains the parent key |
| `hasMany()`       | iterable `Result`           | related table contains the parent key |
| `belongsTo()`     | one `Row` or an empty value | parent table contains the related key |
| `belongsToMany()` | iterable `Result`           | a pivot table contains both keys      |

Pass key names explicitly in namespaced applications. When omitted, the implementation derives keys and pivot names
by lowercasing class strings; fully qualified class names rarely match real database identifiers.

### One to one

```php
use Qubus\Expressive\ActiveRecord\Relations\HasOne;

public function featuredImage(): HasOne
{
    return $this->hasOne(Image::class, 'post_id');
}
```

Accessing the relationship as a property executes it:

```php
$post = Post::find($postId);
$image = $post?->featuredImage;
```

Define the inverse with `belongsTo()` and the foreign key stored on the current model:

```php
use Qubus\Expressive\ActiveRecord\Relations\BelongsTo;

public function post(): BelongsTo
{
    return $this->belongsTo(Post::class, 'post_id');
}
```

The current single-row terminal limitation described under “Retrieve one filtered record” also affects lazy `hasOne`
and `belongsTo` resolution. Verify those relations against the version in use before relying on them for production
queries.

### One to many

```php
use Qubus\Expressive\ActiveRecord\Relations\HasMany;

public function comments(): HasMany
{
    return $this->hasMany(Comment::class, 'post_id');
}
```

```php
$post = Post::find($postId);

if ($post !== null) {
    foreach ($post->comments as $comment) {
        echo $comment->body;
    }
}
```

The inverse belongs on `Comment`:

```php
public function post(): BelongsTo
{
    return $this->belongsTo(Post::class, 'post_id');
}
```

### Many to many

A many-to-many relationship needs a pivot table. The arguments after the related class are the pivot table, the key
for the current model, and the key for the related model:

```php
use Qubus\Expressive\ActiveRecord\Relations\BelongsToMany;

public function tags(): BelongsToMany
{
    return $this->belongsToMany(
        Tag::class,
        'post_tag',
        'post_id',
        'tag_id',
    );
}
```

```php
$post = Post::find($postId);

if ($post !== null) {
    foreach ($post->tags as $tag) {
        echo $tag->name;
    }
}
```

The relation reads pivot rows; it does not provide attach, detach, or pivot-update helpers. Use QueryBuilder operations
on the pivot table for those writes.

### Constrain a relationship

Call a relationship as a method to add filters, ordering, or a limit. End a collection relationship with `get()`:

```php
$comments = $post->comments()
    ->where('status', 'approved')
    ->orderBy('created_at', 'DESC')
    ->limit(5)
    ->get();
```

Reusable constraints can be declared in the relationship itself:

```php
public function approvedComments(): HasMany
{
    return $this->hasMany(Comment::class, 'post_id')
        ->where('status', 'approved');
}
```

Then use `$post->approvedComments` for the complete result, or call `$post->approvedComments()` to add another query
constraint.

## Eager loading

Lazy collection relationships can create an N+1 query pattern: one query for posts followed by one comments query per
post. `Result::load()` preloads a named relationship for the whole result:

```php
$posts = Post::where('status', 'published')->get();
$posts->load('comments');

foreach ($posts as $post) {
    echo $post->title;

    // Use property access to read the cached relationship.
    foreach ($post->comments as $comment) {
        echo $comment->body;
    }
}
```

For `hasMany`, this changes the access pattern from one related query per parent to one related query using `WHERE IN`
for all parent IDs. `load()` mutates the `Result` and returns `void`; it accepts one relationship name at a time and
does not support nested relationship paths.

A single model also has `load($relationship)`. It preloads that relationship on the model but does not reduce queries
across a collection.

## Accessors and mutators

An accessor transforms a stored value when it is read. Name it `getAttr` followed by the attribute name in camel case:

```php
public function getAttrTitle(mixed $value, Model $model): string
{
    return strtoupper((string) $value);
}
```

```php
echo $post->title; // The transformed value.
echo $post->getData('title'); // The raw value held by the model.
```

A mutator transforms a value before the model stores it in its internal data array:

```php
public function setAttrTitle(mixed $value, Model $model): string
{
    return trim((string) $value);
}
```

Both callbacks receive the attribute value and the model. PHP permits declaring only the first parameter when the model
is not needed. Accessors run only for attributes present in the model data; they cannot currently create virtual
attributes for missing columns.

## Read-only models

Set `$isReadOnly` when a model maps a view or another source that must not be changed:

```php
final class MonthlyReport extends Model
{
    protected ?string $tableName = 'monthly_report';
    protected bool $isReadOnly = true;
}
```

Persistence operations on a model throw `ReadOnlyException`. `Row::save()` and `Row::delete()` catch that exception,
write its message to the PHP error log, and return `false`.

## Arrays and JSON

Convert a collection:

```php
$posts = Post::all();

$array = $posts->toArray();
$json = $posts->toJson();
```

Models and rows delegate the same methods:

```php
$post = Post::find($postId);

if ($post !== null) {
    $array = $post->toArray();
    $json = $post->toJson();
}
```

`toJson()` uses pretty-printed JSON and returns `false` if encoding fails. Relationships are included by `toArray()`
only after they have been loaded onto the model. Eager-loaded `hasMany` and `belongsToMany` collections serialize as
nested arrays.

## When to use the QueryBuilder escape hatch

`dbalQuery()` returns a QueryBuilder already configured with the model table, prefix, and primary key:

```php
$rows = Post::dbalQuery()
    ->select(['post_id', 'title'])
    ->where('status', 'published')
    ->find();
```

Use it for joins, a filtered `findOne()`, pivot writes, or query features that Active Record does not wrap cleanly.
QueryBuilder results are not Active Record `Row` or `Result` objects, so Active Record accessors, mutators,
relationships, and serialization are not applied.

## Operational boundaries

Active Record deliberately remains smaller than a full ORM:

- `save()` does not automatically populate `created_at` or `updated_at` columns.
- Models do not validate attributes or cast them from schema metadata; use application validation and
  accessors/mutators.
- Existing-record updates write all data held by the model rather than tracking dirty fields.
- There is no identity map, unit of work, model event system, or second-level cache.
- Relationship writes such as attach, detach, and cascading deletes are not built in.

Use database constraints for invariants the database must always protect, and a service or repository layer when a
workflow needs validation, authorization, transaction coordination, or domain events.
