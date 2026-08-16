# Expressive Database Abstraction Layer

A PDO-based database toolkit with a query builder, schema builder, migrations, Active Record, and DataMapper. Expressive
supports `pdo_mysql`, `pdo_sqlite`, `pdo_oci`, `pdo_pgsql`, and `pdo_sqlsrv`.

## Requirements

- PHP 8.4 or later
- PDO and the PDO extension for the selected database

## Installation

```shell
composer require qubus/expressive
```

## Quick start

```php
<?php

use Qubus\Expressive\Connection\DriverConnection;

$connection = DriverConnection::make('sqlite:///:memory:');
$db = $connection->queryBuilder();

$users = $db->table('users')->where('active', true)->find();
```

Values passed to query methods are bound through PDO. SQL identifiers and expressions—such as table names, column
names, aliases, joins, and custom aggregates—must come from trusted application code.

## More Info

- [Documentation overview](docs/index.md)
- [Connections](docs/connections.md)
- [QueryBuilder](docs/query-builder.md)
- [DataMapper](docs/data-mapper.md)
- [Active Record](docs/active-record.md)
- [Framework Documentation](https://codefyphp.com/docs/database/)
