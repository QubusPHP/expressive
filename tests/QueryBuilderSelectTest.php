<?php

use PHPUnit\Framework\Assert;
use Qubus\Expressive\Connection;
use Qubus\Expressive\QueryBuilderException;

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

it('builds portable empty IN and NOT IN predicates', function () use ($connection) {
    $emptyIn = $connection->queryBuilder()->table('users')->whereIn('user_id', [])->getSelectQuery();
    $emptyNotIn = $connection->queryBuilder()->table('users')->whereNotIn('user_id', [])->getSelectQuery();

    expect($emptyIn)->toContain('WHERE', '0 = 1')
        ->and($emptyNotIn)->toContain('WHERE', '1');
});

it('rejects unsafe ordering and invalid pagination values', function () use ($connection) {
    $builder = $connection->queryBuilder()->table('users');

    expect(fn () => $builder->orderBy('username', 'ASC; DROP TABLE users'))
        ->toThrow(QueryBuilderException::class, 'ASC or DESC')
        ->and(fn () => $builder->limit(-1))
        ->toThrow(QueryBuilderException::class, 'non-negative')
        ->and(fn () => $builder->offset(-1))
        ->toThrow(QueryBuilderException::class, 'non-negative')
        ->and(fn () => $builder->pagination(0, 1))
        ->toThrow(QueryBuilderException::class, 'greater than zero');
});

it('supports a zero limit', function () use ($connection) {
    $query = $connection->queryBuilder()->table('users')->limit(0)->getSelectQuery();

    expect($query)->toContain('LIMIT', '0');
});
