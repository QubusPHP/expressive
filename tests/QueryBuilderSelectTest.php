<?php

use PHPUnit\Framework\Assert;
use Qubus\Expressive\Connection;

/** @var Connection $connection */
$connection = require(__DIR__ . '/bootstrap/bootstrap.php');

it('should build simple select string.', function () use ($connection) {
    $expected = "SELECT my_table.* FROM my_table AS my_table WHERE 1";

    $query = $connection->queryBuilder()->table(tableName: 'my_table')
        ->getSelectQuery();

    $query = preg_replace("/\r|\n/", "", $query);
    $query = preg_replace('/\s+/', ' ', $query);
    $query = trim($query);

    Assert::assertEquals($expected, $query);
});

it('should build select string with LIKE.', function () use ($connection) {
    $expected = "SELECT my_table.* FROM my_table AS my_table WHERE my_table.field LIKE ?";

    $query = $connection
            ->queryBuilder()
            ->table('my_table')
            ->whereLike('field', '%this%')
            ->getSelectQuery();

    $query = preg_replace("/\r|\n/", "", $query);
    $query = preg_replace('/\s+/', ' ', $query);
    $query = trim($query);

    Assert::assertEquals($expected, $query);
});

it('should build select string with comma delimited fields.', function () use ($connection) {
    $expected = "SELECT my_table.column, my_table.other FROM my_table AS my_table WHERE 1";

    $query = $connection
            ->queryBuilder()
            ->table('my_table')
            ->select(['column', 'other'])
            ->getSelectQuery();

    $query = preg_replace("/\r|\n/", "", $query);
    $query = preg_replace('/\s+/', ' ', $query);
    $query = trim($query);

    Assert::assertEquals($expected, $query);
});

it('should build select string with function.', function () use ($connection) {
    $expected = "SELECT COUNT(users.user_id) FROM users AS users WHERE 1";

    $query = $connection
            ->queryBuilder()
            ->table('users')
            ->select('COUNT(users.user_id)')
            ->getSelectQuery();

    $query = preg_replace("/\r|\n/", "", $query);
    $query = preg_replace('/\s+/', ' ', $query);
    $query = trim($query);

    Assert::assertEquals($expected, $query);
});

it('should build select string with aliased function.', function () use ($connection) {
    $expected = "SELECT COUNT(my_table.*) AS num FROM my_table AS my_table WHERE 1";

    $query = $connection
            ->queryBuilder()
            ->table('my_table')
            ->select('COUNT(my_table.*)', 'num')
            ->getSelectQuery();

    $query = preg_replace("/\r|\n/", "", $query);
    $query = preg_replace('/\s+/', ' ', $query);
    $query = trim($query);

    Assert::assertEquals($expected, $query);
});

it('should build select string with where condition.', function () use ($connection) {
    $expected = "SELECT my_table.* FROM my_table AS my_table WHERE my_table.field = ?";

    $query = $connection
            ->queryBuilder()
            ->table('my_table')
            ->where('field', 'value')
            ->getSelectQuery();

    $query = preg_replace("/\r|\n/", "", $query);
    $query = preg_replace('/\s+/', ' ', $query);
    $query = trim($query);

    Assert::assertEquals($expected, $query);
});
