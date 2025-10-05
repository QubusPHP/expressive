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
