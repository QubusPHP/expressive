<?php

use Qubus\Expressive\Connection\Pdo\Sqlite;
use Qubus\Expressive\Schema\CreateTable;

$connection = new Sqlite(config: ['driver' => 'pdo_sqlite', 'dsn' => 'sqlite::memory:']);

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

$schema = $connection->queryBuilder()->schema();
if (!$schema->hasTable(table: 'users')) {
    $schema->create(table: 'users', callback: function (CreateTable $table) {
        $table->string(name: 'user_id', length: 36)
            ->primary()
            ->unique(name: 'userId');
        $table->string(name: 'username', length: 191)
            ->unique(name: 'username')
            ->notNull();
        $table->string(name: 'first_name', length: 191);
        $table->string(name: 'last_name', length: 191);
        $table->string(name: 'email', length: 191)
            ->unique(name: 'email')
            ->notNull();
    });

    foreach ($users as $user) {
        $connection->queryBuilder()->table(tableName: 'users')
            ->insert(data: $user);
    }
}

return $connection;
