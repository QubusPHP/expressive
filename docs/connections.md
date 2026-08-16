# Connections

Expressive uses PDO and supports MySQL, PostgreSQL, SQLite, Oracle, and SQL Server.

## Creating a connection

`DriverConnection::make()` accepts a URI or a configuration array:

```php
<?php

use Qubus\Expressive\Connection\DriverConnection;

$mysql = DriverConnection::make(
    'mysql://app_user:p%40ssword@localhost:3306/app?charset=utf8mb4'
);

$sqlite = DriverConnection::make('sqlite:///:memory:');

$postgres = DriverConnection::make([
    'driver' => 'pdo_pgsql',
    'host' => 'localhost',
    'port' => 5432,
    'dbname' => 'app',
    'username' => 'app_user',
    'password' => 'password',
]);
```

URI usernames, passwords, paths, and query parameters should be URL-encoded. Ports must be integers from 1 through
65535. Unsupported or missing drivers throw `TypeException`.

Supported driver names are `mysql`, `pgsql`, `sqlite`, `oci`, and `sqlsrv`, optionally prefixed with `pdo_` in a
configuration array.

## Executing parameterized SQL

Use positional or named parameters instead of interpolating values:

```php
$result = $connection->query(
    'SELECT * FROM users WHERE email = :email',
    ['email' => 'person@example.com']
);

$row = $result->first();

$updated = $connection->command(
    'UPDATE users SET active = ? WHERE user_id = ?',
    [true, 42]
);
```

Other helpers include `command()` for non-query statements and `column()` for the first column of the first row.
Integer, boolean, null, and string parameter types are bound with their corresponding PDO types.

When a statement fails, `DbalException` retains the original exception and includes the SQL template. Bound values are
not interpolated into the exception message, preventing sensitive values from leaking into logs.

## Transactions

Use `transactional()` for automatic commit and rollback:

```php
$connection->transactional(function () use ($connection): void {
    $connection->command(
        'INSERT INTO users (username, email) VALUES (?, ?)',
        ['new-user', 'new-user@example.com']
    );
});
```

Any `Throwable` escaping the callback triggers rollback and is rethrown. Nested calls use savepoints.

The legacy `transaction()` helper is also available on PDO connection implementations. It passes the selected context
object to its callback and now rolls back for every `Throwable`, not only `PDOException`.

## Multiple connections

Connections and their query builders are isolated. Creating a builder for a second connection does not reuse or mutate
the first connection:

```php
$primaryDb = $primaryConnection->queryBuilder();
$reportingDb = $reportingConnection->queryBuilder();
```

Keep the appropriate builder or connection in the service that owns each database boundary.
