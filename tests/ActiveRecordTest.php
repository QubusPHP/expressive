<?php

use PHPUnit\Framework\Assert;
use Qubus\Expressive\Connection;

/** @var Connection $connection */
$connection = require(__DIR__ . '/bootstrap/bootstrap.php');

Qubus\Tests\Expressive\ActiveRecord\User::connection(connection: $connection);

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

it(description: 'should return user array that matches user model', closure: function () use ($users) {
    $items = Qubus\Tests\Expressive\ActiveRecord\User::all()->toArray();
    Assert::assertEquals(expected: $users, actual: $items);
});

it(description: 'should return the same QueryBuilder results', closure: function () use ($connection) {
    $items = $connection->queryBuilder()->table(tableName: 'users')->find();
    $activeRecord = Qubus\Tests\Expressive\ActiveRecord\User::dbalQuery()->find();
    Assert::assertEquals(expected: $items, actual: $activeRecord);
});
