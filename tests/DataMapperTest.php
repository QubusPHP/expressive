<?php

use PHPUnit\Framework\Assert;
use Qubus\Expressive\Connection;
use Qubus\Expressive\DataMapper\DataMapperException;
use Qubus\Expressive\DataMapper\PdoDataMapper;

/** @var Connection $connection */
$connection = require(__DIR__ . '/bootstrap/bootstrap.php');

$users = [
    [
        'user_id' => '01K6TYWFJCEFVA8E5ZRF0CGSHV',
        'username' => 'user1',
        'first_name' => 'First',
        'last_name' => 'User',
        'email' => 'user1@gmail.com'
    ],
    [
        'user_id' => '01K6TYX0XPE1KVWHC1QNA1NYMP',
        'username' => 'user2',
        'first_name' => 'Second',
        'last_name' => 'User',
        'email' => 'user2@gmail.com'
    ],
    [
        'user_id' => '01K6TYXTJJECE9MCPHC0YT4RSV',
        'username' => 'user3',
        'first_name' => 'Third',
        'last_name' => 'User',
        'email' => 'user3@gmail.com'
    ],
    [
        'user_id' => '01K6TYY5SME3TT193FQ1YFS848',
        'username' => 'user4',
        'first_name' => 'Fourth',
        'last_name' => 'User',
        'email' => 'user4@gmail.com'
    ]
];

it(
    description: 'should return user array that matches user datamapper',
    closure: function () use ($users, $connection) {
        $mapper = new PdoDataMapper(connection: $connection, entity: \Qubus\Tests\Expressive\DataMapper\User::class);

        $items = $mapper->findAll();

        $hydrate = $mapper->hydrate($users);
        Assert::assertEquals(expected: $hydrate, actual: $items);
    }
);

it(description: 'should return the same QueryBuilder results', closure: function () use ($connection) {
    $items = $connection->queryBuilder()->table(tableName: 'users')->find();

    $dataMapper = new PdoDataMapper(connection: $connection, entity: \Qubus\Tests\Expressive\DataMapper\User::class);
    Assert::assertEquals(expected: $items, actual: $dataMapper->queryBuilder()->find());
});

it('finds one entity and filters by a mapped property', function () use ($connection) {
    $mapper = new PdoDataMapper($connection, \Qubus\Tests\Expressive\DataMapper\User::class);

    $user = $mapper->findOne('01K6TYX0XPE1KVWHC1QNA1NYMP');
    $matches = $mapper->findAllBy('login', 'user2');

    expect($user)->not->toBeNull()
        ->and($user->login)->toBe('user2')
        ->and($matches)->toHaveCount(1)
        ->and(array_values($matches)[0]->email)->toBe('user2@gmail.com');
});

it('rejects unsafe or invalid query options', function () use ($connection) {
    $mapper = new PdoDataMapper($connection, \Qubus\Tests\Expressive\DataMapper\User::class);

    expect(fn () => $mapper->findAll(options: ['direction' => 'ASC; DROP TABLE users']))
        ->toThrow(DataMapperException::class, 'Sort direction')
        ->and(fn () => $mapper->findAll(orderBy: 'password'))
        ->toThrow(DataMapperException::class, 'Unknown entity property')
        ->and(fn () => $mapper->findAll(options: ['limit' => -1]))
        ->toThrow(DataMapperException::class, 'non-negative');

    expect($connection->queryBuilder()->schema()->hasTable('users'))->toBeTrue();
});

it('rejects incomplete hydration rows', function () use ($connection) {
    $mapper = new PdoDataMapper($connection, \Qubus\Tests\Expressive\DataMapper\User::class);

    expect(fn () => $mapper->hydrate([['user_id' => 'missing-fields']]))
        ->toThrow(DataMapperException::class, 'is missing');
});

it('creates, updates, and deletes entities with client-generated ids', function () use ($connection) {
    $mapper = new PdoDataMapper($connection, \Qubus\Tests\Expressive\DataMapper\User::class);
    $user = new \Qubus\Tests\Expressive\DataMapper\User();
    $user->id = '01KDATAMAPPER00000000000001';
    $user->login = 'mapper-user';
    $user->fname = 'Mapper';
    $user->lname = 'User';
    $user->email = 'mapper-user@gmail.com';

    expect($mapper->create($user))->toBe($user)
        ->and($mapper->findOne($user->id)?->login)->toBe('mapper-user');

    $user->email = 'updated-mapper-user@gmail.com';
    expect($mapper->update($user))->toBe($user)
        ->and($mapper->findOne($user->id)?->email)->toBe('updated-mapper-user@gmail.com');

    $mapper->delete($user->id);
    expect($mapper->findOne($user->id))->toBeNull();
});

it('reports uninitialized entity properties before executing SQL', function () use ($connection) {
    $mapper = new PdoDataMapper($connection, \Qubus\Tests\Expressive\DataMapper\User::class);
    $user = new \Qubus\Tests\Expressive\DataMapper\User();
    $user->id = '01KINCOMPLETE000000000000001';

    expect(fn () => $mapper->create($user))
        ->toThrow(DataMapperException::class, 'property login is not initialized');
});
