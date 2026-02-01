<?php

declare(strict_types=1);

namespace Qubus\Expressive;

use ArrayIterator;
use Closure;
use DateTime;
use Exception;
use IteratorAggregate;
use PDO;
use PDOException;
use PDOStatement;
use Qubus\Inheritance\TapObjectAware;
use SplFixedArray;
use Stringable;
use Traversable;

use function array_fill;
use function array_key_exists;
use function array_keys;
use function array_map;
use function array_merge;
use function array_shift;
use function array_splice;
use function array_unique;
use function array_values;
use function count;
use function explode;
use function func_get_args;
use function func_num_args;
use function implode;
use function is_array;
use function is_callable;
use function is_numeric;
use function preg_match;
use function preg_replace_callback;
use function Qubus\Support\Helpers\is_null__;
use function sprintf;
use function str_replace;
use function strpbrk;
use function strpos;
use function strtoupper;
use function trim;

use const COUNT_RECURSIVE;

class QueryBuilder implements IteratorAggregate, Stringable, Database
{
    use TapObjectAware;

    // Directional filter
    public const string ORDERBY_ASC = 'ASC';
    public const string ORDERBY_DESC = 'DESC';

    // Breaks
    public const string EOL = "\n";
    public const string TAB = "\t";
    public const string EOL_TAB = "\n\t";

    protected static ?QueryBuilder $instance = null;
    //phpcs:disable
    protected Connection|null $connection = null {
        get => $this->connection;
    }
    //phpcs:enable
    protected ?string $tableName = null;
    protected string $tableToken = '';
    protected string $tableAlias = '';
    protected bool $isSingle = false;
    protected ?PDOStatement $pdoStmt = null;
    protected array $selectFields = [];
    protected array $joinSources = [];
    protected ?int $limit = null;
    protected ?int $offset = null;
    /** @var array<string> $orderBy */
    protected array $orderBy = [];
    /** @var array<string> $groupBy */
    protected array $groupBy = [];
    /** @var array<mixed> $whereParameters */
    protected array $whereParameters = [];
    /** @var array<mixed> $whereConditions */
    protected array $whereConditions = [];
    protected string $andOrOperator = self::OPERATOR_AND;
    /** @var array<mixed> $having */
    protected array $having = [];
    protected ?string $returning = null;
    /** @var array<mixed>|null $upsert */
    protected ?array $upsert = null;
    protected bool $wrapOpen = false;
    protected int $lastWrapPosition = 0;
    protected bool $isFluentQuery = true;
    protected bool $pdoExecuted = false;
    /** @var array<mixed> $data */
    protected array $data = [];
    protected bool $debugSqlQuery = false;
    protected string $sqlQuery = '';
    /** @var array<mixed> $sqlParameters */
    protected array $sqlParameters = [];
    /** @var array<string> $dirtyFields */
    protected array $dirtyFields = [];
    /** @var array<int|string, array> $referenceKeys */
    protected array $referenceKeys = [];
    protected bool $joinOn = false;
    protected static array $references = [];
    protected ?string $tablePrefix = null;
    /** @var ?Schema $schema */
    protected ?Schema $schema = null;
    /** @var array<string> $tableStructure */
    public array $tableStructure = [
        'primaryKeyname' => 'id',
        'foreignKeyname' => '%s_id',
    ];

    /**
     * Constructor & set the table structure
     *
     * @param Connection $connection Database connection.
     * @param string|null $tablePrefix Prefix of database tables.
     * @param string $primaryKeyName Structure: table primary key. If it's an array, it must be the structure
     * @param string $foreignKeyName Structure: table foreignKeyName.
     *                               It can be like %s_id where %s is the table name
     */
    public function __construct(
        Connection $connection,
        ?string $tablePrefix = null,
        string $primaryKeyName = 'id',
        string $foreignKeyName = '%s_id'
    ) {
        $this->connection = $connection;
        $this->setStructure(primaryKeyName: $primaryKeyName, foreignKeyName: $foreignKeyName);
        $this->setTablePrefix(tablePrefix: $tablePrefix);
    }

    /**
     * {@inheritDoc}
     */
    public static function fromInstance(
        Connection $connection,
        string $primaryKeyName = 'id',
        ?string $tablePrefix = null
    ): self {
        if (static::$instance === null) {
            static::$instance = new self($connection)
                ->setStructure(primaryKeyName: $primaryKeyName)
                ->setTablePrefix(tablePrefix: $tablePrefix);
        }
        return static::$instance;
    }

    /**
     * {@inheritDoc}
     */
    public function table(string $tableName, ?string $alias = null): self
    {
        $instance = clone $this;

        $newTableName = null !== $instance->getTablePrefix() ? $instance->getTablePrefix() . $tableName : $tableName;

        $instance->tableName = $newTableName;
        $instance->tableToken = $newTableName;
        $instance->setTableAlias(alias: $alias ?? $newTableName);
        $instance->reset();
        return $instance;
    }

    /**
     * {@inheritDoc}
     */
    public function getTableName(): string
    {
        return $this->tableName;
    }

    /**
     * {@inheritDoc}
     */
    public function setTableAlias(string $alias): self
    {
        $this->tableAlias = $alias;
        return $this;
    }

    /**
     * {@inheritDoc}
     */
    public function getTableAlias(): string
    {
        return $this->tableAlias;
    }

    /**
     * {@inheritDoc}
     */
    public function setStructure(
        string $primaryKeyName = 'id',
        string $foreignKeyName = '%s_id'
    ): self {
        $this->tableStructure = [
            'primaryKeyname' => $primaryKeyName,
            'foreignKeyname' => $foreignKeyName,
        ];
        return $this;
    }

    /**
     * {@inheritDoc}
     */
    public function setTablePrefix(?string $tablePrefix = ''): self
    {
        $this->tablePrefix = $tablePrefix;
        return $this;
    }

    /**
     * {@inheritDoc}
     */
    public function getTablePrefix(): ?string
    {
        return $this->tablePrefix;
    }

    /**
     * {@inheritDoc}
     */
    public function getStructure(): array
    {
        return $this->tableStructure;
    }

    /**
     * {@inheritDoc}
     */
    public function getPrimaryKeyname(): string
    {
        return $this->formatKeyname(pattern: $this->tableStructure['primaryKeyname'], tablename: $this->tableName);
    }

    /**
     * {@inheritDoc}
     */
    public function getForeignKeyname(): string
    {
        return $this->formatKeyname(pattern: $this->tableStructure['foreignKeyname'], tablename: $this->tableName);
    }

    /**
     * Return if the entry is of a single row
     */
    public function isSingleRow(): bool
    {
        return $this->isSingle;
    }

    /**
     * {@inheritDoc}
     */
    public function query(
        string $query,
        array $parameters = [],
        bool $returnAsPdoStmt = false
    ): self|PDOStatement {
        $this->sqlParameters = $parameters;
        $this->sqlQuery = $query;

        if ($this->debugSqlQuery) {
            return $this;
        } else {
            $this->pdoStmt = $this->connection->pdo->prepare(query: $query);
            $this->pdoExecuted = $this->pdoStmt->execute(params: $parameters);
            if ($returnAsPdoStmt) {
                return $this->pdoStmt;
            } else {
                $this->isFluentQuery = true;
                return $this;
            }
        }
    }

    /**
     * Return the number of affected row by the last statement
     */
    public function rowCount(): int
    {
        return $this->pdoExecuted === true ? $this->pdoStmt->rowCount() : 0;
    }

    /* ------------------------------------------------------------------------------
      Querying
     * ----------------------------------------------------------------------------- */

    /**
     * {@inheritDoc}
     */
    public function find(?callable $callback = null): mixed
    {
        if ($this->isFluentQuery && $this->pdoStmt === null) {
            $this->query(query: $this->getSelectQuery(), parameters: $this->getWhereParameters());
        }

        //Debug SQL Query
        if ($this->debugSqlQuery) {
            return $this->getSqlQuery();
        }

        if ($this->pdoExecuted === true) {
            $allRows = $this->pdoStmt->fetchAll(mode: PDO::FETCH_ASSOC);
            $this->reset();
            if (is_callable(value: $callback)) {
                return $callback($allRows);
            } else {
                if (count($allRows)) {
                    // Holding all foreign keys matching the structure
                    $matchForeignKey = function ($key) {
                        return preg_match(
                            pattern: '/' . str_replace(
                                search: '%s',
                                replace: '[a-z]',
                                subject: $this->tableStructure['foreignKeyname']
                            ) . '/i',
                            subject: $key
                        );
                    };
                    foreach ($allRows as $index => &$row) {
                        if ($index === 0) {
                            $this->referenceKeys = [$this->tableStructure['primaryKeyname'] => []];
                            foreach (array_keys(array: $row) as $_rowK) {
                                if ($matchForeignKey($_rowK)) {
                                    $this->referenceKeys[$_rowK] = [];
                                }
                            }
                        }
                        foreach ($row as $rowK => &$rowV) {
                            if (array_key_exists(key: $rowK, array: $this->referenceKeys)) {
                                $this->referenceKeys[$rowK][] = $rowV;
                                $this->referenceKeys[$rowK] = array_unique(array: $this->referenceKeys[$rowK]);
                            }
                        }
                    }
                    unset($row);
                    $rows = [];
                    foreach ($allRows as $row) {
                        $rows[] = $this->fromArray(data: $row);
                    }
                    $splFa = SplFixedArray::fromArray(array: $rows);
                    $fixed = $splFa->getIterator();
                    unset($rows);
                    return $fixed;
                }
                return new ArrayIterator();
            }
        } else {
            return false;
        }
    }

    /**
     * {@inheritDoc}
     */
    public function findOne(int|string|null $id = null): self|bool
    {
        if ($id) {
            $this->wherePK(id: $id);
        }
        $this->limit(limit: 1);
        // Debug the SQL Query
        if ($this->debugSqlQuery) {
            $this->find();
            return false;
        } else {
            $findAll = $this->find();
            while ($findAll->valid()) {
                return $findAll->current();
            }
            return false;
        }
    }

    /**
     * This method allow the iteration inside foreach().
     *
     * @return ArrayIterator
     */
    public function getIterator(): Traversable
    {
        return $this->isSingle ? new ArrayIterator(array: $this->toArray()) : $this->find();
    }

    /**
     * {@inheritDoc}
     */
    public function fromArray(array $data): self
    {
        $row = clone $this;
        $row->reset();
        $row->isSingle = true;
        $row->data = $data;
        return $row;
    }

    /* ------------------------------------------------------------------------------
      Fluent Query Builder
     * ----------------------------------------------------------------------------- */
    /**
     * {@inheritDoc}
     */
    public function select(mixed $columns = '*', ?string $alias = null): self
    {
        $this->isFluentQuery = true;
        if ($alias && ! is_array(value: $columns)) {
            $columns .= " AS {$alias} ";
        }
        if (is_array(value: $columns)) {
            $this->selectFields = array_merge($this->selectFields, $columns);
        } else {
            $this->selectFields[] = $columns;
        }
        return $this;
    }

    /**
     * {@inheritDoc}
     */
    public function where(mixed $condition, mixed $parameters = null): self
    {
        $this->isFluentQuery = true;

        // By default, the andOrOperator and wrap operator is AND,
        if ($this->wrapOpen || ! $this->andOrOperator) {
            $this->and();
        }

        // where( ["column1" => 1, "column2 > ?" => 2] )
        if (is_array(value: $condition)) {
            foreach ($condition as $key => $val) {
                $this->where(condition: $key, parameters: $val);
            }
            return $this;
        }
        $column = $condition;
        $args = func_num_args();
        if ($args !== 2 || strpbrk(string: $condition, characters: '?:')) { // where("column < ? OR column > ?", [1, 2])
            $column = explode(separator: ' ', string: trim(string: $condition))[0];
            if ($args !== 2 || ! is_array(value: $parameters)) { // where("column < ? OR column > ?", 1, 2)
                $parameters = func_get_args();
                array_shift($parameters);
            }
        } elseif (! is_array(value: $parameters)) { //where(colum,value) => colum=value
            $condition .= ' = ?';
            $parameters = [$parameters];
        } else { // where("column", [1, 2]) => column IN (?,?)
            $placeholders = $this->makePlaceholders(numberOfPlaceholders: count($parameters));
            $condition = "({$condition} IN ({$placeholders}))";
        }

        $this->whereConditions[] = [
            'COLUMN' => $column,
            'STATEMENT' => $condition,
            'PARAMS' => $parameters,
            'OPERATOR' => $this->andOrOperator,
        ];

        // Reset the where operator to AND. To use OR, you must call or__()
        $this->and();

        return $this;
    }

    /**
     * {@inheritDoc}
     */
    public function and(): self
    {
        if ($this->wrapOpen) {
            $this->whereConditions[] = self::OPERATOR_AND;
            $this->lastWrapPosition = count($this->whereConditions);
            $this->wrapOpen = false;
        } else {
            $this->andOrOperator = self::OPERATOR_AND;
        }
        return $this;
    }

    /**
     * {@inheritDoc}
     */
    public function or(): self
    {
        if ($this->wrapOpen) {
            $this->whereConditions[] = self::OPERATOR_OR;
            $this->lastWrapPosition = count($this->whereConditions);
            $this->wrapOpen = false;
        } else {
            $this->andOrOperator = self::OPERATOR_OR;
        }
        return $this;
    }

    /**
     * {@inheritDoc}
     */
    public function wrap(): self
    {
        $this->wrapOpen = true;

        $spliced = array_splice(
            $this->whereConditions,
            offset: $this->lastWrapPosition,
            length: count($this->whereConditions),
            replacement: '('
        );
        $this->whereConditions = array_merge($this->whereConditions, $spliced);

        $this->whereConditions[] = ')';
        $this->lastWrapPosition = count($this->whereConditions);

        return $this;
    }

    /**
     * {@inheritDoc}
     */
    public function wherePK(int|string $id): self
    {
        return $this->where(condition: $this->getPrimaryKeyname(), parameters: $id);
    }

    /**
     * {@inheritDoc}
     */
    public function whereNot(string $columnName, mixed $value): self
    {
        return $this->where(condition: "$columnName != ?", parameters: $value);
    }

    /**
     * {@inheritDoc}
     */
    public function whereLike(string $columnName, mixed $value): self
    {
        return $this->where(condition: "$columnName LIKE ?", parameters: $value);
    }

    /**
     * {@inheritDoc}
     */
    public function whereNotLike(string $columnName, mixed $value): self
    {
        return $this->where(condition: "$columnName NOT LIKE ?", parameters: $value);
    }

    /**
     * {@inheritDoc}
     */
    public function whereGt(string $columnName, mixed $value): self
    {
        return $this->where(condition: "$columnName > ?", parameters: $value);
    }

    /**
     * {@inheritDoc}
     */
    public function whereGte(string $columnName, mixed $value): self
    {
        return $this->where(condition: "$columnName >= ?", parameters: $value);
    }

    /**
     * {@inheritDoc}
     */
    public function whereLt(string $columnName, mixed $value): self
    {
        return $this->where(condition: "$columnName < ?", parameters: $value);
    }

    /**
     * {@inheritDoc}
     */
    public function whereLte(string $columnName, mixed $value): self
    {
        return $this->where(condition: "$columnName <= ?", parameters: $value);
    }

    /**
     * {@inheritDoc}
     */
    public function whereIn(string $columnName, array $values): self
    {
        return $this->where(condition: $columnName, parameters: $values);
    }

    /**
     * {@inheritDoc}
     */
    public function whereNotIn(string $columnName, array $values): self
    {
        $placeholders = $this->makePlaceholders(numberOfPlaceholders: count($values));

        return $this->where(condition: "({$columnName} NOT IN ({$placeholders}))", parameters: $values);
    }

    /**
     * {@inheritDoc}
     */
    public function whereNull(string $columnName): self
    {
        return $this->where(condition: "({$columnName} IS NULL)");
    }

    /**
     * {@inheritDoc}
     */
    public function whereNotNull(string $columnName): self
    {
        return $this->where(condition: "({$columnName} IS NOT NULL)");
    }

    public function having($statement, string $operator = self::OPERATOR_AND): self
    {
        $this->isFluentQuery = true;
        $this->having[] = [
            'STATEMENT' => $statement,
            'OPERATOR' => $operator,
        ];
        return $this;
    }

    /**
     * {@inheritDoc}
     */
    public function orderBy(string $columnName, string $ordering = 'ASC'): self
    {
        $this->isFluentQuery = true;
        $this->orderBy[] = "{$columnName} {$ordering}";
        return $this;
    }

    /**
     * {@inheritDoc}
     */
    public function groupBy(string $columnName): self
    {
        $this->isFluentQuery = true;
        $this->groupBy[] = $columnName;
        return $this;
    }

    /**
     * {@inheritDoc}
     */
    public function limit(?int $limit = null): self|int|null
    {
        if ($limit) {
            $this->isFluentQuery = true;
            $this->limit = $limit;
            return $this;
        } else {
            return $this->limit;
        }
    }

    /**
     * {@inheritDoc}
     */
    public function offset(?int $offset = null): self|int|null
    {
        if ($offset) {
            $this->isFluentQuery = true;
            $this->offset = $offset;
            return $this;
        } else {
            return $this->offset;
        }
    }

    /**
     * {@inheritDoc}
     */
    public function pagination(int $perPage, int $page): self
    {
        $this->limit = $perPage;
        $this->offset = (($page > 0 ? $page : 1) - 1) * $perPage;

        return $this;
    }

    /* ------------------------------------------------------------------------------
      JOIN
     * ----------------------------------------------------------------------------- */

    /**
     * {@inheritDoc}
     */
    public function join(
        string $tableName,
        string $constraint,
        string $tableAlias = '',
        string $joinOperator = self::JOIN_LEFT
    ): self {
        $this->isFluentQuery = true;
        $join = trim(string: "{$joinOperator} JOIN");
        $join .= self::EOL_TAB;
        $join .= "AS {$tableAlias} ";
        $join .= self::EOL_TAB . self::TAB;
        $join .= "ON ({$constraint})";
        $join .= self::EOL;
        $this->joinSources[] = $join;
        return $this;
    }

    /**
     * {@inheritDoc}
     */
    public function on(Database $query, string $joinOperator = self::JOIN_LEFT): self
    {
        $this->joinOn = true;
        $constraint = str_replace(
            search: '%join.',
            replace: $this->getTableAlias() . 'expressive',
            subject: $query->getJoinOnString()
        );

        $this->select(
            columns: array_map(callback: function ($row) {
                return str_replace('%join.', $this->getTableAlias() . 'expressive', $row);
            }, array: $query->getSelectFields())
        );

        return $this->join(
            tableName: $query->getTableName(),
            constraint: $constraint,
            tableAlias: $query->getTableAlias(),
            joinOperator: $joinOperator
        );
    }

    /* ------------------------------------------------------------------------------
      Utils
     * ----------------------------------------------------------------------------- */

    /**
     * Return the built select query
     *
     * @return string
     */
    public function getSelectQuery(): string
    {
        $query = [
            'SELECT',
            self::EOL_TAB,
            $this->getSelectString(),
            self::EOL,
            'FROM',
            self::EOL_TAB,
            $this->getTableName(),
            'AS',
            $this->getTableAlias(),
            self::EOL,
            $this->getJoinString(),
            self::EOL,
            'WHERE',
            self::EOL_TAB,
            $this->getWhereString(),
            self::EOL,
        ];

        if (! count($this->groupBy) && $this->joinOn) {
            $this->groupBy(columnName: '%this.' . $this->getPrimaryKeyname());
        }
        if (count($this->groupBy)) {
            $query[] = 'GROUP BY';
            $query[] = self::EOL_TAB;
            $query[] = $this->getGroupbyString();
            $query[] = self::EOL;
        }
        if (count($this->orderBy)) {
            $query[] = 'ORDER BY';
            $query[] = self::EOL_TAB;
            $query[] = $this->getOrderbyString();
            $query[] = self::EOL;
        }
        if (count($this->having)) {
            $query[] = 'HAVING';
            $query[] = self::EOL_TAB;
            $query[] = $this->getHavingString();
            $query[] = self::EOL;
        }
        if ($this->limit) {
            $query[] = 'LIMIT';
            $query[] = self::EOL_TAB;
            $query[] = $this->limit;
            $query[] = self::EOL;
        }
        if ($this->offset) {
            $query[] = 'OFFSET';
            $query[] = self::EOL_TAB;
            $query[] = $this->offset;
            $query[] = self::EOL;
        }
        return $this->formatColumnName(column: implode(separator: ' ', array: $query));
    }

    /**
     * {@inheritDoc}
     */
    public function schema(): Schema
    {
        if (is_null__(var: $this->schema)) {
            $this->schema = $this->connection->getSchema();
        }

        return $this->schema;
    }

    /**
     * Get the select fields as string for SQL.
     *
     * @return string
     */
    public function getSelectString(): string
    {
        return implode(separator: ', ' . self::EOL_TAB, array: $this->getSelectFields());
    }

    /**
     * {@inheritDoc}
     */
    public function getSelectFields(): array
    {
        if (! count($this->selectFields)) {
            $this->select(columns: '*');
        }
        return $this->prepareColumns(columns: $this->selectFields);
    }

    /**
     * Get a JOIN string.
     *
     * @return string
     */
    public function getJoinString(): string
    {
        return ' ' . implode(separator: ' ', array: $this->joinSources);
    }

    /**
     * Get the group by string.
     *
     * @return string
     */
    public function getGroupbyString(): string
    {
        return implode(separator: ', ', array: array_unique(array: $this->groupBy));
    }

    /**
     * Get the order by string.
     *
     * @return string
     */
    public function getOrderbyString(): string
    {
        return implode(separator: ', ', array: array_unique(array: $this->orderBy));
    }

    /**
     * Build the WHERE clause(s).
     *
     * @return string
     */
    public function getWhereString(): string
    {
        // If there are no WHERE clauses, return empty string
        if (! count($this->whereConditions)) {
            return '1';
        }

        $whereCondition = '';
        $lastCondition = '';

        foreach ($this->whereConditions as $condition) {
            if (is_array(value: $condition)) {
                if (
                    $whereCondition
                    && $lastCondition !== "(" && ! preg_match(pattern: "/\)\s+(OR|AND)\s+$/i", subject: $whereCondition)
                ) {
                    $whereCondition .= $condition['OPERATOR'];
                }
                $whereCondition .= $condition['STATEMENT'];
                $this->whereParameters = array_merge($this->whereParameters, $condition['PARAMS']);
            } else {
                $whereCondition .= $condition;
            }
            $lastCondition = $condition;
        }

        $columns = [];
        foreach ($this->whereConditions as $condition) {
            $column = $condition['COLUMN'];
            $columns[$column] = !str_contains($column, '.') ? "%this.{$column}" : $column;
        }
        $stmt = str_replace(
            search: array_keys(array: $columns),
            replace: array_values($columns),
            subject: $whereCondition
        );
        return $this->formatColumnName(column: $stmt);
    }

    /**
     * {@inheritDoc}
     */
    public function getJoinOnString(): string
    {
        $where = $this->getWhereString();

        $params = $this->whereParameters;
        return preg_replace_callback(pattern: '/\?/', callback: function ($match) use (&$params) {
            $arg = array_shift($params);
            if (is_numeric(value: $arg)) {
                return $arg;
            } elseif (str_contains($arg, '%')) {
                return $arg;
            } else {
                return "'{$arg}'";
            }
        }, subject: $where);
    }

    /**
     * Return the HAVING clause.
     *
     * @return string
     */
    protected function getHavingString(): string
    {
        // If there are no WHERE clauses, return empty string
        if (! count($this->having)) {
            return '';
        }

        $havingCondition = '';

        foreach ($this->having as $condition) {
            if (is_array(value: $condition)) {
                if ($havingCondition && ! preg_match(pattern: "/\)\s+(OR|AND)\s+$/i", subject: $havingCondition)) {
                    $havingCondition .= $condition['OPERATOR'];
                }
                $havingCondition .= $condition['STATEMENT'];
            } else {
                $havingCondition .= $condition;
            }
        }
        return $havingCondition;
    }

    /**
     * Return the values to be bound for where.
     *
     * @return array
     */
    protected function getWhereParameters(): array
    {
        return $this->whereParameters;
    }

    /**
     * Detect if it's a single row instance and reset it to PK.
     *
     * @return $this
     */
    protected function setSingleWhere(): self
    {
        if ($this->isSingle) {
            $this->resetWhere();
            $this->wherePK(id: $this->getPK());
        }
        return $this;
    }

    /**
     * Reset the where.
     *
     * @return $this
     */
    protected function resetWhere(): self
    {
        $this->whereConditions = [];
        $this->whereParameters = [];
        return $this;
    }

    /* ------------------------------------------------------------------------------
      Insert
     * ----------------------------------------------------------------------------- */

    /**
     * {@inheritDoc}
     */
    public function returning(string $cols = '*'): self
    {
        $this->returning = $cols;

        return $this;
    }

    /**
     * {@inheritDoc}
     */
    public function upsert(array $conflictCols, array $updateData): self
    {
        $this->upsert = [
            'conflict' => $conflictCols,
            'update'   => $updateData
        ];
        return $this;
    }

    /**
     * {@inheritDoc}
     */
    public function lastInsertId(string|null $pk = null): string|false
    {
        if ($this->connection->pdo->getAttribute(attribute: PDO::ATTR_DRIVER_NAME) === 'pgsql') {
            $pk = sprintf('%s_%s_seq', $this->getTableName(), $pk);
        }
        return $this->connection->pdo->lastInsertId(name: $pk);
    }

    /**
     * {@inheritDoc}
     */
    public function insert(array $data): self|int
    {
        $insertValues = [];
        $questionMarks = [];

        // check if the data is multi dimension for bulk insert
        $multi = $this->isArrayMultiDim(data: $data);

        $datafield = array_keys(array: $multi ? $data[0] : $data);

        if ($multi) {
            foreach ($data as $d) {
                $questionMarks[] = '(' . $this->makePlaceholders(numberOfPlaceholders: count($d)) . ')';
                $insertValues = array_merge($insertValues, array_values(array: $d));
            }
        } else {
            $questionMarks[] = '(' . $this->makePlaceholders(numberOfPlaceholders: count($data)) . ')';
            $insertValues = array_values(array: $data);
        }

        $sql = "INSERT INTO {$this->tableName} (" . implode(separator: ',', array: $datafield) . ") ";
        $sql .= "VALUES " . implode(separator: ',', array: $questionMarks);

        if ($this->connection->supportsReturning()) {
            $sql .= " RETURNING {$this->returning}";
        }

        if ($this->connection->supportsUpsert() && $this->upsert !== null) {
            if (str_contains($this->connection->pdo->getAttribute(attribute: PDO::ATTR_DRIVER_NAME), 'pgsql')) {
                $conf = implode(separator: ',', array: $this->upsert['conflict']);
                $updates = implode(
                    separator: ', ',
                    array: array_map(
                        callback: fn($col) => "{$col}=EXCLUDED.{$col}",
                        array: array_keys($this->upsert['update'])
                    )
                );
                $sql .= " ON CONFLICT ({$conf}) DO UPDATE SET {$updates}";
            } elseif (str_contains($this->connection->pdo->getAttribute(attribute: PDO::ATTR_DRIVER_NAME), 'mysql')) {
                $updates = implode(
                    separator: ', ',
                    array: array_map(
                        callback: fn($col) => "{$col}=VALUES({$col})",
                        array: array_keys($this->upsert['update'])
                    )
                );
                $sql .= " ON DUPLICATE KEY UPDATE {$updates}";
            }
        }

        $this->query(query: $sql, parameters: $insertValues);

        // Return the SQL Query
        if ($this->debugSqlQuery) {
            $this->debugSqlQuery(bool: false);
            return $this;
        }

        $rowCount = $this->rowCount();

        // On single element return the object
        if ($rowCount === 1) {
            $primaryKeyname = $this->getPrimaryKeyname();
            $data[$primaryKeyname] = $this->lastInsertId(pk: $primaryKeyname);
            return $this->fromArray(data: $data);
        }

        return $rowCount;
    }

    /* ------------------------------------------------------------------------------
      Updating
     * ----------------------------------------------------------------------------- */

    /**
     * {@inheritDoc}
     */
    public function update(?array $data = null): self|int|false
    {
        $this->setSingleWhere();

        if (! is_null__(var: $data)) {
            $this->set(key: $data);
        }

        // Make sure we remove the primary key
        unset($this->dirtyFields[$this->getPrimaryKeyname()]);

        $values = array_values(array: $this->dirtyFields);
        $fieldList = [];

        if (count($values) === 0) {
            return false;
        }

        foreach (array_keys(array: $this->dirtyFields) as $key) {
            $fieldList[] = "{$key} = ?";
        }

        $query = [
            'UPDATE',
            self::EOL_TAB,
            $this->getTableName(),
            'AS',
            $this->getTableAlias(),
            self::EOL,
            'SET',
            self::EOL_TAB,
            implode(separator: ', ', array: $fieldList),
            self::EOL,
            'WHERE',
            self::EOL_TAB,
            $this->getWhereString(),
            self::EOL,
        ];
        $this->query(
            query: implode(separator: ' ', array: $query),
            parameters: array_merge($values, $this->getWhereParameters())
        );

        // Return the SQL Query
        if ($this->debugSqlQuery) {
            $this->debugSqlQuery(bool: false);
            return $this;
        } else {
            $this->dirtyFields = [];
            return $this->rowCount();
        }
    }

    /* ------------------------------------------------------------------------------
      Delete
     * ----------------------------------------------------------------------------- */

    /**
     * {@inheritDoc}
     */
    public function delete(bool $deleteAll = false): self|int|false
    {
        $this->setSingleWhere();

        if (count($this->whereConditions)) {
            $query = [
                'DELETE FROM',
                self::EOL_TAB,
                $this->getTableName(),
                self::EOL,
                'WHERE',
                self::EOL_TAB,
                $this->getWhereString(),
                self::EOL,
            ];
            $this->query(implode(separator: ' ', array: $query), $this->getWhereParameters());
        } elseif ($deleteAll) {
            $query = "DELETE FROM {$this->tableName}";
            $this->query(query: $query);
        } else {
            return false;
        }

        // Return the SQL Query
        if ($this->debugSqlQuery) {
            $this->debugSqlQuery(bool: false);
            return $this;
        } else {
            return $this->rowCount();
        }
    }

    /* ------------------------------------------------------------------------------
      PDO Transactions
     * ----------------------------------------------------------------------------- */

    /**
     * Initiates a transaction.
     *
     * @return bool
     */
    public function beginTransaction(): bool
    {
        return $this->connection->pdo->beginTransaction();
    }

    /**
     * Checks if inside transaction.
     *
     * @return bool
     */
    public function inTransaction(): bool
    {
        return $this->connection->pdo->inTransaction();
    }

    /**
     * Commits a transaction
     *
     * @return bool
     */
    public function commit(): bool
    {
        return $this->connection->pdo->commit();
    }

    /**
     * Rolls back a transaction.
     *
     * @return bool
     */
    public function rollBack(): bool
    {
        return $this->connection->pdo->rollBack();
    }

    /**
     * {@inheritDoc}
     */
    public function transactional(Closure $callback, mixed $that = null, mixed $default = null): mixed
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
            $this->beginTransaction();

            // execute the callback
            $result = $callback($this);

            // all fine, commit the transaction
            $this->commit();
        } catch (PDOException $e) { // catch any errors generated in the callback
            // rollback on error
            $this->rollBack();
            throw new QueryBuilderException(message: $e->getMessage(), code: (int) $e->getCode());
        }

        return $result;
    }

    /* ------------------------------------------------------------------------------
      Set & Save
     * ----------------------------------------------------------------------------- */

    /**
     * {@inheritDoc}
     */
    public function set(mixed $key, mixed $value = null): self
    {
        if (is_array(value: $key)) {
            foreach ($key as $keyKey => $keyValue) {
                $this->set(key: $keyKey, value: $keyValue);
            }
        } elseif ($key !== $this->getPrimaryKeyname()) {
            $this->data[$key] = $value;
            $this->dirtyFields[$key] = $value;
        }
        return $this;
    }

    /**
     * {@inheritDoc}
     */
    public function save(): self|int|bool
    {
        if ($this->isSingle || count($this->whereConditions)) {
            return $this->update();
        } else {
            return $this->insert(data: $this->dirtyFields);
        }
    }

    /* ------------------------------------------------------------------------------
      AGGREGATION
     * ----------------------------------------------------------------------------- */
    /**
     * {@inheritDoc}
     */
    public function count(?string $column = null): float|int
    {
        if (! $column) {
            $column = $this->getPrimaryKeyname();
        }
        return $this->aggregate(fn: "COUNT({$this->prepareColumn(column: $column)})");
    }

    /**
     * {@inheritDoc}
     */
    public function max(string $column): float|int
    {
        return $this->aggregate(fn: "MAX({$this->prepareColumn(column: $column)})");
    }

    /**
     * {@inheritDoc}
     */
    public function min(string $column): float|int
    {
        return $this->aggregate(fn: "MIN({$this->prepareColumn(column: $column)})");
    }

    /**
     * {@inheritDoc}
     */
    public function sum(string $column): float|int
    {
        return $this->aggregate(fn: "SUM({$this->prepareColumn(column: $column)})");
    }

    /**
     * {@inheritDoc}
     */
    public function avg(string $column): float|int
    {
        return $this->aggregate(fn: "AVG({$this->prepareColumn(column: $column)})");
    }

    /**
     * {@inheritDoc}
     */
    public function aggregate(string $fn): float|int
    {
        $this->select(columns: $fn, alias: 'count');
        $result = $this->findOne();
        return $result !== false && isset($result->count) ? $result->count : 0;
    }

    /* ------------------------------------------------------------------------------
      Access single entry data
     * ----------------------------------------------------------------------------- */
    /**
     * Return the primary key.
     *
     * @return int|string|null
     */
    public function getPK(): int|string|null
    {
        return $this->get(key: $this->getPrimaryKeyname());
    }

    /**
     * Get the key
     *
     * @param string $key
     * @return mixed
     */
    public function get(string $key): mixed
    {
        return $this->data[$key] ?? null;
    }

    /**
     * Return the raw data of this single instance.
     *
     * @return array
     */
    public function toArray(): array
    {
        return $this->data;
    }

    public function __get($key)
    {
        return $this->get(key: $key);
    }

    public function __set($key, $value)
    {
        $this->set(key: $key, value: $value);
    }

    public function __isset($key)
    {
        return isset($this->data[$key]);
    }

    /* ------------------------------------------------------------------------------
      Association
     * ----------------------------------------------------------------------------- */
    /**
     * Association / Load
     *
     * __call() will load a table by association or return the table object itself
     *
     * To dynamically call a table
     *
     * $db = new QueryBuilder($myPDO);
     * on table 'users'
     * $Users = $db->table("users");
     *
     * Or to call a table association
     * on table 'photos' where users can have many photos
     * $allMyPhotos = $Users->findOne(1234)->photos();
     *
     * Association allow you to associate the current table with another by using
     * foreignKey and localKey. The data is eagerly loaded hence only making one round to the table
     * to retrieve the data matching the foreign and protected keys
     * foreign and protected keys are cached for subsequent queries,
     * the keys are selected based on the foreignKeyname pattern.
     * i.e: having the keys: id, user_id, friend_id, name, last_name
     * id, user_id, friend_id will be cached so they can be queried upon request
     *
     * @param  array $args
     *      foreignKey
     *      localKey
     *      where
     *      sort
     *      callback
     *      model
     *      backref
     */
    public function __call(string $tablename, array $args)
    {
        $def = [
            'model' => null, // An instance QueryBuilder class as the class to interact with
            'foreignKey' => '', // the foreign key for the association
            'localKey' => '', // localKey for the association
            'columns' => '*', // the columns to select
            'where' => [], // Where condition
            'sort' => '', // Sort of the result
            'callback' => null, // A callback on the results
            'backref' => false, // When true, it will query in the reverse direction
        ];

        $prop = array_merge($def, $args);
        $tableName = $this->getTableName() ?: $tablename;

        return $prop['model'] ? : $this->table(tableName: $tableName);
    }

    // Utilities methods

    /**
     * Reset fields
     *
     * @return $this
     */
    public function reset(): self
    {
        $this->whereParameters = [];
        $this->selectFields = [];
        $this->joinSources = [];
        $this->whereConditions = [];
        $this->limit = null;
        $this->offset = null;
        $this->orderBy = [];
        $this->groupBy = [];
        $this->data = [];
        $this->dirtyFields = [];
        $this->isFluentQuery = true;
        $this->andOrOperator = self::OPERATOR_AND;
        $this->having = [];
        $this->wrapOpen = false;
        $this->lastWrapPosition = 0;
        $this->debugSqlQuery = false;
        $this->pdoStmt = null;
        $this->isSingle = false;
        $this->joinOn = false;
        return $this;
    }

    /**
     * Return an Immutable YYYY-MM-DD HH:II:SS date format
     *
     * @param string $datetime - An english textual datetime description
     *          now, yesterday, 3 days ago, +1 week
     *          http://php.net/manual/en/function.strtotime.php
     * @return string YYYY-MM-DD HH:II:SS
     * @throws Exception
     */
    public static function now(string $datetime = 'now'): string
    {
        return new DateTime(datetime: $datetime ?: 'now')->format(format: 'Y-m-d H:i:s');
    }

    // QueryBuilder Debugger

    /**
     * To debug the query. It will not execute it but instead using debugSqlQuery()
     * and getSqlParameters to get the data
     *
     * @param bool $bool
     * @return $this
     */
    public function debugSqlQuery(bool $bool = true): self
    {
        $this->debugSqlQuery = $bool;
        return $this;
    }

    /**
     * Get the SQL Query with
     *
     * @return string
     */
    public function getSqlQuery(): string
    {
        return $this->sqlQuery;
    }

    /**
     * Return the parameters of the SQL
     *
     * @return array
     */
    public function getSqlParameters(): array
    {
        return $this->sqlParameters;
    }

    public function __clone()
    {
    }

    public function __toString(): string
    {
        return $this->isSingle ? (string) $this->getPK() : $this->tableName;
    }

    /**
     * Return a string containing the given number of question marks,
     * separated by commas. Eg "?, ?, ?"
     *
     * @param int $numberOfPlaceholders - total of placeholder to insert.
     * @return string
     */
    protected function makePlaceholders(int $numberOfPlaceholders = 1): string
    {
        return implode(separator: ', ', array: array_fill(start_index: 0, count: $numberOfPlaceholders, value: '?'));
    }

    /**
     * Format the table{Primary|Foreign}KeyName
     *
     * @param string $pattern
     * @param string $tablename
     * @return string
     */
    protected function formatKeyname(string $pattern, string $tablename): string
    {
        return sprintf($pattern, $tablename);
    }

    /**
     * To create a string that will be used as key for the relationship.
     *
     * @param  string  $key
     * @param string $suffix
     * @return string
     */
    protected function tokenize(string $key, string $suffix = ''): string
    {
        return $this->tableToken . ":$key:$suffix";
    }

    /**
     * Check if array is multi dim.
     *
     * @param array $data
     * @return bool
     */
    protected function isArrayMultiDim(array $data): bool
    {
        return count($data) !== count($data, COUNT_RECURSIVE);
    }

    /**
     * Prepare columns to include the table alias name.
     *
     * @param array $columns
     * @return array
     */
    protected function prepareColumns(array $columns): array
    {
        $newColumns = [];
        foreach ($columns as $column) {
            if (strpos($column, ',')) {
                $newColumns = array_merge(
                    $this->prepareColumns(columns: explode(separator: ',', string: $column)),
                    $newColumns
                );
            } else {
                $newColumns[] = $this->prepareColumn(column: $column);
            }
        }
        return $newColumns;
    }

    protected function prepareColumn(string $column): string
    {
        $column = trim(string: $column);
        if (
                !str_contains($column, '.')
                && !str_contains(strtoupper(string: $column), 'NULL')
        ) {
            if (! preg_match('/^[0-9]/', $column)) {
                $column = "%this.{$column}";
            }
        }
        return $this->formatColumnName($column);
    }

    /**
     * Format a column name to add to the table alias.
     *
     * @param string $column
     * @return string
     */
    public function formatColumnName(string $column): string
    {
        return str_replace(search: '%this.', replace: $this->getTableAlias() . 'expressive', subject: $column);
    }
}
