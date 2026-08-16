# QueryBuilder

Expressive includes a fluent SQL query builder. It can return mutable row objects for convenient single-row updates, but
it is not a full ORM. Use Active Record or DataMapper when you need model or entity abstractions.

## Creating a query builder

Create a connection and request a query builder from it:

```php
<?php

use Qubus\Expressive\Connection\DriverConnection;

$connection = DriverConnection::make(
    'mysql://app_user:password@localhost:3306/app?charset=utf8mb4'
);
$db = $connection->queryBuilder();

$posts = $db->table('posts');
```

Table names may also be selected dynamically:

```php
$posts = $db->posts();
```

`table()` is clearer for static analysis and is recommended when the table name is known.

Each builder created by `queryBuilder()` or `QueryBuilder::fromInstance()` remains bound to the connection supplied to
it. Builders do not share a cached connection.

## Security and input handling

Query values passed through `where()`, `query()`, or `raw()` are bound as PDO parameters. Do not concatenate untrusted
values into SQL.

Table names, column names, aliases, join constraints, select expressions, aggregate expressions, and other SQL
fragments are identifiers or SQL—not values. They must come from trusted application code or an allowlist.

`orderBy()` accepts only `ASC` or `DESC` as its direction. Invalid directions, negative limits or offsets, non-positive
pagination sizes, empty insert payloads, and inconsistent bulk-insert rows throw `QueryBuilderException`.

## Transactions

`transactional()` commits when its callback completes and rolls back when any `Throwable` escapes the callback. The
builder is passed to the callback:

```php
<?php

use Qubus\Expressive\Database;

$db->transactional(function (Database $db): void {
    $db->table('users')->insert([
        'username' => 'new-user',
        'email' => 'new-user@example.com',
    ]);
});
```

Nested transactions use savepoints when supported by the driver.

## Insert

Pass one associative array to insert one row. A successful single insert returns a row-style `QueryBuilder` instance.
When an explicit primary key is supplied, the returned row preserves it; otherwise the builder uses `lastInsertId()`.

```php
<?php

$post = $posts->insert([
    'title' => 'Expressive QueryBuilder',
    'content' => 'Building parameterized database queries.',
    'author_id' => 1,
    'published_at' => $posts->now(),
]);

echo $post->title;
```

Pass a list of associative arrays for a bulk insert. Every row must contain the same columns in the same order. A bulk
insert returns the number of affected rows.

```php
<?php

$inserted = $posts->insert([
    [
        'title' => 'Domain-driven frameworks',
        'content' => 'Domain-driven frameworks started...',
        'author_id' => 1,
        'published_at' => $posts->now(),
    ],
    [
        'title' => 'Modern PHP',
        'content' => 'New language features include...',
        'author_id' => 1,
        'published_at' => $posts->now(),
    ],
]);
```

An empty insert is rejected.

## Update

Set the `WHERE` clause before calling `update()`:

```php
<?php

$affected = $posts
    ->where('post_id', 3847)
    ->update([
        'title' => 'Updated title',
    ]);
```

You may also use `set()`:

```php
$affected = $posts
    ->set('title', 'Updated title')
    ->where('post_id', 3847)
    ->update();
```

A row returned by `findOne()` can be updated directly:

```php
$post = $posts->findOne(3847);

if ($post !== false) {
    $post->title = 'Updated title';
    $affected = $post->save();
}
```

`update()` returns the number of affected rows, `false` when there is nothing to update, or the builder in SQL-debug
mode.

## Save

`save()` inserts a new builder with dirty fields and updates a single-row or filtered builder:

```php
<?php

$newPost = $db->table('posts');
$newPost->title = 'A new post';
$newPost->content = 'Post content';
$newPost->save();

$existingPost = $posts->findOne(3847);
if ($existingPost !== false) {
    $existingPost->title = 'Revised post';
    $existingPost->save();
}
```

## Delete

Delete a single row:

```php
<?php

$post = $posts->findOne(3847);
if ($post !== false) {
    $post->delete();
}
```

Delete matching rows:

```php
$affected = $posts
    ->where('title', 'Obsolete post')
    ->delete();
```

For safety, `delete()` returns `false` when no `WHERE` clause is present. A deliberate full-table delete must call
`delete(deleteAll: true)`.

## Querying

### Find one

`findOne()` returns a single row-style builder or `false`:

```php
<?php

$post = $posts
    ->where('slug', 'expressive-query-builder')
    ->findOne();

$post = $posts->findOne(364); // Uses the configured primary key.
```

Configure a non-default primary key before selecting the table:

```php
$posts = $db
    ->setStructure(primaryKeyName: 'post_id')
    ->table('posts');
```

### Find many

`find()` returns an iterator of row-style builders. It returns `false` only when no statement was executed:

```php
<?php

$results = $posts
    ->where('title', 'Modern PHP')
    ->find();

if ($results !== false) {
    foreach ($results as $post) {
        echo $post->content;
    }
}
```

`find()` also accepts a callback. The callback receives the fetched rows as associative arrays, and its return value is
returned to the caller:

```php
$results = $posts
    ->whereLike('title', 'PHP%')
    ->find(static function (array $rows): array {
        return array_map(
            static fn (array $row): string => $row['title'],
            $rows
        );
    });
```

## Select

Without an explicit selection, the builder selects all columns from the current table:

```php
$posts->select();
```

Select individual columns with an array or comma-delimited string:

```php
$posts->select(['title', 'content']);

$posts
    ->select('title, content')
    ->select('published_at');
```

Expressions such as `COUNT(posts.post_id)` are accepted, but must be trusted SQL.

## Where clauses

Repeated filters are combined with `AND` by default. Values are parameterized:

```php
<?php

$posts->where('title', 'Modern PHP');
$posts->where('author_id > ?', 25);
$posts->where('category IN (?, ?, ?)', ['PHP', 'HTML', 'Ruby']);
$posts->where([
    'status' => 'published',
    'author_id > ?' => 25,
]);
```

Convenience methods include:

```php
$posts->wherePK(456);
$posts->whereNot('post_id', 24);
$posts->whereLike('title', 'PHP%');
$posts->whereNotLike('title', 'Draft%');
$posts->whereGt('published_at', '2025-01-01');
$posts->whereGte('published_at', '2025-01-01');
$posts->whereLt('published_at', '2026-01-01');
$posts->whereLte('published_at', '2026-01-01');
$posts->whereIn('author_id', [2, 24]);
$posts->whereNotIn('author_id', [2, 24]);
$posts->whereNull('content');
$posts->whereNotNull('published_at');
```

`whereIn($column, [])` produces an always-false predicate. `whereNotIn($column, [])` adds no predicate, which is the
logical always-true result and avoids invalid `IN ()` SQL on stricter database engines.

### AND, OR, and grouping

```php
$posts
    ->where('author_id', 24)
    ->and()
    ->whereGte('published_at', '2025-01-01');

$posts
    ->where('author_id', 24)
    ->or()
    ->where('category', 'Programming');
```

Use `wrap()` when grouped predicates are required.

## Order, group, limit, offset, and pagination

```php
<?php

$posts->orderBy(columnName: 'post_id', ordering: 'DESC');
$posts->groupBy(columnName: 'category');
$posts->limit(limit: 10);
$posts->offset(offset: 10);
$posts->pagination(perPage: 25, page: 2);
```

Ordering is case-insensitive but limited to `ASC` and `DESC`. Limit and offset must be non-negative. `limit(0)` is
valid and returns no rows. `pagination()` requires a value greater than zero for `perPage` and treats pages below one as
page one.

## Aggregates

Aggregate methods honor the current filters:

```php
$count = $posts->where('status', 'published')->count();
$count = $posts->count('author_id');
$max = $posts->max('post_id');
$min = $posts->min('post_id');
$sum = $posts->sum('view_count');
$average = $posts->avg('view_count');
$custom = $posts->aggregate('COUNT(DISTINCT posts.author_id)');
```

Custom aggregate expressions are raw SQL and must not contain untrusted input.

## Joins

`join()` defaults to `LEFT JOIN`:

```php
<?php

$posts->join(
    tableName: 'categories',
    constraint: 'categories.category_id = posts.category_id',
    tableAlias: 'categories'
);
```

Supply a different supported SQL join operator with the `joinOperator` argument. Join constraints are raw SQL and must
come from trusted application code.

## Direct SQL

Use `raw()` when an associative-array result is wanted:

```php
$rows = $db->raw(
    'SELECT title FROM posts WHERE author_id = :author_id',
    ['author_id' => 24]
);
```

Use `query()` to retain builder/PDO-statement behavior:

```php
$statement = $db->query(
    'SELECT title FROM posts WHERE author_id = ?',
    [24],
    returnAsPdoStmt: true
);
```

Both positional and named parameters are supported. Database exceptions include the SQL template for diagnostics but
do not interpolate bound values, which prevents credentials and other sensitive values from leaking into logs.

