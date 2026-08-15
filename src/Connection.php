<?php

declare(strict_types=1);

namespace Qubus\Expressive;

use Closure;
use PDO;
use Qubus\Expressive\Schema\Compiler;

interface Connection
{
    //phpcs:disable
    public PDO $pdo { get; }
    //phpcs:enable

    public function query(string $sql, array $params = []): ResultSet;

    public function command(string $sql, array $params = []): bool;

    public function column(string $sql, array $params = []): mixed;

    public function queryBuilder(): QueryBuilder;

    public function getSchema(): Schema;

    public function schemaCompiler(): Compiler;

    public function getDsn(): ?string;

    public function quoteIdentifier(string $identifier): string;

    /** @return list<string> */
    public function listTables(): array;

    public function transactional(Closure $callback): mixed;

    /**
     * Driver feature detection.
     *
     * @return bool
     */
    public function supportsReturning(): bool;

    /**
     * @return bool
     */
    public function supportsUpsert(): bool;
}
