<?php

declare(strict_types=1);

use Qubus\Expressive\Connection\Pdo\Sqlite;
use Qubus\Expressive\DbalException;
use Qubus\Expressive\QueryBuilder;

/** @var Sqlite $connection */
$connection = require __DIR__ . '/bootstrap/bootstrap.php';

it('binds named parameters through the connection API', function () use ($connection) {
    $row = $connection->query(
        'SELECT username FROM users WHERE email = :email',
        ['email' => 'user2@gmail.com']
    )->first();

    expect($row)->toBe(['username' => 'user2']);
});

it('rolls back transactions for every throwable', function () use ($connection) {
    $callback = function () use ($connection): void {
        $connection->command(
            'INSERT INTO users (user_id, username, first_name, last_name, email) VALUES (?, ?, ?, ?, ?)',
            ['rollback-id', 'rollback-user', 'Rollback', 'User', 'rollback@gmail.com']
        );

        throw new RuntimeException('stop');
    };

    expect(fn () => $connection->transaction($callback))->toThrow(RuntimeException::class, 'stop')
        ->and($connection->inTransaction())->toBeFalse()
        ->and($connection->column('SELECT COUNT(*) FROM users WHERE user_id = ?', ['rollback-id']))->toBe(0);
});

it('does not expose bound values in database exceptions', function () use ($connection) {
    $secret = 'database-secret-value';

    try {
        $connection->command(
            'INSERT INTO users (user_id, username, first_name, last_name, email) VALUES (?, ?, ?, ?, ?)',
            ['duplicate-id', 'user1', 'Duplicate', 'User', $secret]
        );
    } catch (DbalException $exception) {
        expect($exception->getMessage())->not->toContain($secret)
            ->and($exception->getPrevious())->not->toBeNull();

        return;
    }

    test()->fail('Expected a database exception.');
});

it('escapes identifier delimiters', function () use ($connection) {
    expect($connection->quoteIdentifier('odd"name'))->toBe('"odd""name"');
});

it('keeps query builder instances isolated by connection', function () {
    $first = new Sqlite(['driver' => 'pdo_sqlite', 'dsn' => 'sqlite::memory:']);
    $second = new Sqlite(['driver' => 'pdo_sqlite', 'dsn' => 'sqlite::memory:']);
    $first->command('CREATE TABLE markers (value TEXT NOT NULL)');
    $second->command('CREATE TABLE markers (value TEXT NOT NULL)');
    $first->command('INSERT INTO markers (value) VALUES (?)', ['first']);
    $second->command('INSERT INTO markers (value) VALUES (?)', ['second']);

    $firstRows = QueryBuilder::fromInstance($first)->raw('SELECT value FROM markers');
    $secondRows = QueryBuilder::fromInstance($second)->raw('SELECT value FROM markers');

    expect($firstRows)->toBe([['value' => 'first']])
        ->and($secondRows)->toBe([['value' => 'second']]);
});
