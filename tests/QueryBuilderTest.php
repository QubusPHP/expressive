<?php

use PHPUnit\Framework\Assert;
use Qubus\Expressive\Connection;
use Qubus\Expressive\Database;

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

it(description: 'should create a PDO connection', closure: function () use ($connection) {
    Assert::assertInstanceOf(expected: PDO::class, actual: $connection->pdo);
});

it(description: 'should return Database instance', closure: function () use ($connection) {
    Assert::assertInstanceOf(expected: Database::class, actual: $connection->queryBuilder());
});

it(description: 'should return user array that matches', closure: function () use ($users, $connection) {
    $items = $connection->queryBuilder()->table(tableName: 'users')->find(function ($data) {
        $array = [];
        foreach ($data as $d) {
            $array[] = $d;
        }

        return $array;
    });
    Assert::assertEquals(expected: $users, actual: $items);
});

it('prepares scalar values without corrupting named placeholder prefixes', function () use ($connection) {
    $builder = $connection->queryBuilder();
    $sql = $builder->prepare(
        'SELECT :id AS id, :id2 AS id2, :missing AS missing, :enabled AS enabled',
        ['id' => 1, 'id2' => 20, 'missing' => null, 'enabled' => false]
    );

    expect($sql)->toBe('SELECT 1 AS id, 20 AS id2, NULL AS missing, 0 AS enabled');
});

it('quotes strings when preparing positional values', function () use ($connection) {
    $sql = $connection->queryBuilder()->prepare('SELECT ? AS value', "value' OR 1=1 --");

    expect($sql)->toBe("SELECT 'value'' OR 1=1 --' AS value");
});

it('preserves explicit primary keys on insert results', function () use ($connection) {
    $id = '01KQUERYBUILDER000000000001';
    $inserted = $connection->queryBuilder()->setStructure('user_id')->table('users')->insert([
        'user_id' => $id,
        'username' => 'query-builder-user',
        'first_name' => 'Query',
        'last_name' => 'Builder',
        'email' => 'query-builder-user@gmail.com',
    ]);

    expect($inserted->getPK())->toBe($id);
});

it('validates insert payload shapes', function () use ($connection) {
    $builder = $connection->queryBuilder()->table('users');

    expect(fn () => $builder->insert([]))
        ->toThrow(\Qubus\Expressive\QueryBuilderException::class, 'cannot be empty')
        ->and(fn () => $builder->insert([
            ['username' => 'first', 'email' => 'first@gmail.com'],
            ['email' => 'second@gmail.com', 'username' => 'second'],
        ]))->toThrow(\Qubus\Expressive\QueryBuilderException::class, 'same columns');
});
