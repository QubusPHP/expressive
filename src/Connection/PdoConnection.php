<?php

declare(strict_types=1);

namespace Qubus\Expressive\Connection;

use Closure;
use Exception;
use PDO;
use PDOException;
use PDOStatement;
use Qubus\Expressive\QueryBuilder;
use Qubus\Expressive\ResultSet;
use Qubus\Expressive\Schema;
use Qubus\Expressive\Traits\IdentifierAware;
use Qubus\Expressive\Connection;
use Qubus\Expressive\DbalException;
use RuntimeException;

use function Qubus\Support\Helpers\is_null__;
use function sprintf;

abstract class PdoConnection implements Connection
{
    use IdentifierAware;

    //phpcs:disable
    public ?PDO $pdo = null {
        get => $this->pdo;
    }
    public string $driverName {
        get => 'pdo';
    }
    //phpcs:enable

    protected array $config = [];

    protected ?string $driver = null;

    protected ?Schema\Compiler $schemaCompiler = null;

    /** @var ?Schema $schema Schema instance */
    protected ?Schema $schema = null;

    /** @var array $schemaCompilerOptions Schema compiler options */
    protected array $schemaCompilerOptions = [];

    public function __construct(array $config)
    {
        $this->config = $config;
        $this->pdo = $this->createPdo($config);
    }

    protected function createPdo(array $config): PDO
    {
        $dsn = $config['dsn'] ?? static::buildDsn($config);
        $user = $config['username'] ?? null;
        $pass = $config['password'] ?? null;
        $options = $config['options'] ?? [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
        ];

        try {
            $pdo = new PDO($dsn, $user, $pass, $options);
        } catch (PDOException $e) {
            throw new RuntimeException(sprintf("PDO Connection failed: %s", $e->getMessage()), 0, $e);
        }

        return $pdo;
    }

    /**
     * Returns the DSN associated with this connection
     *
     * @return string|null
     */
    public function getDsn(): ?string
    {
        return $this->config['dsn'] ?? null;
    }

    /**
     * Execute a query
     *
     * @param string $sql SQL Query.
     * @param array $params (optional) Query params.
     * @return ResultSet
     */
    public function query(string $sql, array $params = []): ResultSet
    {
        $prepared = $this->prepare(query: $sql, params: $params);
        $this->pdoExecute(prepared: $prepared);
        return new ResultSet(statement: $prepared['statement']);
    }

    /**
     * Execute a non-query SQL command.
     *
     * @param string $sql SQL Command.
     * @param array $params (optional) Command params.
     * @return bool
     */
    public function command(string $sql, array $params = []): bool
    {
        return $this->pdoExecute(prepared: $this->prepare(query: $sql, params: $params));
    }

    /**
     * Execute a query and return the number of affected rows.
     *
     * @param string $sql SQL Query.
     * @param array $params (optional) Query params.
     * @return  int
     */
    public function affectedRows(string $sql, array $params = []): int
    {
        $prepared = $this->prepare(query: $sql, params: $params);
        $this->pdoExecute(prepared: $prepared);
        $result = $prepared['statement']->rowCount();
        $prepared['statement']->closeCursor();
        return $result;
    }

    /**
     * Execute a query and fetch the first column
     *
     * @param   string $sql SQL Query
     * @param   array $params (optional) Query params
     * @return  mixed
     */
    public function column(string $sql, array $params = []): mixed
    {
        $prepared = $this->prepare(query: $sql, params: $params);
        $this->pdoExecute(prepared: $prepared);
        $result = $prepared['statement']->fetchColumn();
        $prepared['statement']->closeCursor();
        return $result;
    }

    public function queryBuilder(): QueryBuilder
    {
        return new QueryBuilder($this);
    }

    /**
     * Returns the driver's name.
     *
     * @return string|null
     */
    public function getDriver(): ?string
    {
        if (is_null__($this->driver)) {
            $this->driver = $this->config['driver'] ?? $this->pdo->getAttribute(attribute: PDO::ATTR_DRIVER_NAME);
        }

        return $this->driver;
    }

    /**
     * Returns the schema associated with this connection
     */
    public function getSchema(): Schema
    {
        if (is_null__($this->schema)) {
            $this->schema = new Schema(connection: $this);
        }

        return $this->schema;
    }

    /**
     * Returns an instance of the schema compiler associated with this connection
     *
     * @throws Exception
     */
    public function schemaCompiler(): Schema\Compiler
    {
        if (is_null__($this->schemaCompiler)) {
            $this->schemaCompiler = match ($this->getDriver()) {
                'pdo_mysql' => new Schema\Compiler\MySQL(connection: $this),
                'pdo_pgsql' => new Schema\Compiler\PostgreSQL(connection: $this),
                'pdo_sqlsrv' => new Schema\Compiler\SQLServer(connection: $this),
                'pdo_sqlite' => new Schema\Compiler\SQLite(connection: $this),
                'pdo_oci' => new Schema\Compiler\Oracle(connection: $this),
                default => throw new Exception(message: 'Schema not supported yet.'),
            };

            $this->schemaCompiler->setOptions(options: $this->schemaCompilerOptions);
        }

        return $this->schemaCompiler;
    }

    public function inTransaction(): bool
    {
        return $this->pdo->inTransaction();
    }

    /**
     * Start a transaction.
     */
    public function startTransaction(): static
    {
        $this->pdo->beginTransaction();

        return $this;
    }

    public function commitTransaction(): static
    {
        $this->pdo->commit();

        return $this;
    }

    public function rollbackTransaction(): static
    {
        $this->pdo->rollBack();

        return $this;
    }

    /**
     * Run transactional queries.
     *
     * @param Closure $callback transaction callback
     * @throws Exception
     */
    public function transaction(Closure $callback, mixed $that = null, mixed $default = null)
    {
        if (is_null__($that)) {
            $that = $this;
        }

        // check if we are in a transaction
        if ($this->inTransaction()) {
            return $callback($that);
        }

        $result = $default;

        try {
            // start the transaction
            $this->startTransaction();

            // execute the callback
            $result = $callback($this);

            // all fine, commit the transaction
            $this->commitTransaction();
        } catch (PDOException $e) { // catch any errors generated in the callback
            // rollback on error
            $this->rollbackTransaction();
            throw new Exception(message: $e->getMessage(), code: (int) $e->getCode());
        }

        return $result;
    }

    /**
     * Sets the connection encoding.
     *
     * @param string $charset Encoding.
     */
    protected function setCharset(string $charset): void
    {
        if (! empty($charset)) {
            $this->pdo->exec(statement: "SET NAMES {$this->quote(value: $charset)}");
        }
    }

    /**
     * Replace placeholders with parameters.
     *
     * @param string $query SQL query
     * @param array $params Query parameters
     */
    protected function replaceParams(string $query, array $params): string
    {
        return preg_replace_callback(pattern: '/\?/', callback: function () use (&$params) {
            $param = array_shift($params);
            $param = is_object(value: $param) ? get_class(object: $param) : $param;

            if (is_int(value: $param) || is_float(value: $param)) {
                return $param;
            } elseif (is_null__(var: $param)) {
                return 'null';
            } elseif (is_bool(value: $param)) {
                return $param ? 'true' : 'false';
            } else {
                return $this->pdo->quote(string: $param);
            }
        }, subject: $query);
    }

    /**
     * Prepares a query.
     *
     * @param   string $query SQL query
     * @param   array $params Query parameters
     * @return  array
     */
    protected function prepare(string $query, array $params): array
    {
        try {
            $statement = $this->pdo->prepare(query: $query);
        } catch (PDOException $e) {
            throw new DbalException(
                message: $e->getMessage() . ' [ ' . $this->replaceParams(query: $query, params: $params) . ' ] ',
                code: (int) $e->getCode(),
                previous: $e->getPrevious()
            );
        }

        return ['query' => $query, 'params' => $params, 'statement' => $statement];
    }

    /**
     * @param PDOStatement $statement
     * @param array $values
     */
    protected function bindValues(PDOStatement $statement, array $values): void
    {
        foreach ($values as $key => $value) {
            $param = PDO::PARAM_STR;

            if (is_null__(var: $value)) {
                $param = PDO::PARAM_NULL;
            } elseif (is_int(value: $value)) {
                $param = PDO::PARAM_INT;
            } elseif (is_bool(value: $value)) {
                $param = PDO::PARAM_BOOL;
            }

            $statement->bindValue(param: $key + 1, value: $value, type: $param);
        }
    }

    /**
     * Executes a prepared query and returns true on success or false on failure.
     *
     * @param   array $prepared Prepared query
     * @return  bool
     */
    protected function pdoExecute(array $prepared): bool
    {
        try {
            if ($prepared['params']) {
                $this->bindValues(statement: $prepared['statement'], values: $prepared['params']);
            }
            $result = $prepared['statement']->execute();
        } catch (PDOException $e) {
            throw new DbalException(message: $e->getMessage() . ' [ ' . $this->replaceParams(
                query: $prepared['query'],
                params: $prepared['params']
            ) . ' ] ', code: (int) $e->getCode(), previous: $e->getPrevious());
        }

        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function supportsReturning(): bool
    {
        return match ($this->driverName) {
            'pgsql', 'oci', 'sqlsrv' => true,
            default => false,
        };
    }

    /**
     * {@inheritDoc}
     */
    public function supportsUpsert(): bool
    {
        return match ($this->driverName) {
            'mysql', 'pgsql' => true,
            default => false,
        };
    }

    public function supportsSavepoints(): bool
    {
        return match ($this->driverName) {
            'sqlite' => ($this->config['path'] ?? '') !== ':memory:',
            default  => true,
        };
    }

    // Must be implemented by each driver
    abstract public static function buildDsn(array $config): string;
}
