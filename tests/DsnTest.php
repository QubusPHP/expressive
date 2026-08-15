<?php

declare(strict_types=1);

use Qubus\Exception\Data\TypeException;
use Qubus\Expressive\Connection\DriverConnection;
use Qubus\Expressive\Connection\Pdo\Sqlite;
use Qubus\Expressive\ParsePdoDsn;

it('parses URI credentials and excludes them from the PDO DSN', function () {
    $dsn = ParsePdoDsn::fromString(
        'mysql://user%40example.com:p%40ss%3Aword@localhost:3306/app?charset=utf8mb4'
    );

    expect($dsn->driver())->toBe('mysql')
        ->and($dsn->username())->toBe('user@example.com')
        ->and($dsn->password())->toBe('p@ss:word')
        ->and($dsn->port())->toBe(3306)
        ->and($dsn->toPdoDsn())->toBe('mysql:host=localhost;port=3306;dbname=app;charset=utf8mb4');
});

it('parses SQLite URI paths and memory databases', function () {
    $file = ParsePdoDsn::fromString('sqlite:///tmp/my%20database.sqlite');
    $memory = ParsePdoDsn::fromString('sqlite:///:memory:');

    expect($file->path())->toBe('/tmp/my database.sqlite')
        ->and($file->toPdoDsn())->toBe('sqlite:/tmp/my database.sqlite')
        ->and($memory->path())->toBe(':memory:');
});

it('validates ports and connection drivers', function () {
    expect(fn () => ParsePdoDsn::fromString('mysql:host=localhost;port=invalid')->port())
        ->toThrow(InvalidArgumentException::class, 'between 1 and 65535')
        ->and(fn () => DriverConnection::make([]))
        ->toThrow(TypeException::class, 'driver must be provided');
});

it('creates SQLite connections from URI strings', function () {
    $connection = DriverConnection::make('sqlite:///:memory:');

    expect($connection)->toBeInstanceOf(Sqlite::class)
        ->and($connection->getDsn())->toBeNull();
});
