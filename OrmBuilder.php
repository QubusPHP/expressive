<?php

/**
 * Qubus\Expressive
 *
 * @link       https://github.com/QubusPHP/expressive
 * @copyright  2022
 * @author     Joshua Parker <joshua@joshuaparker.dev>
 * @license    https://opensource.org/licenses/mit-license.php MIT License
 *
 * @since      0.1.0
 */

declare(strict_types=1);

namespace Qubus\Expressive;

use ArrayIterator;
use DateTime;
use Exception;
use InternalIterator;
use IteratorAggregate;
use PDO;
use PDOStatement;
use Qubus\Dbal\Connection;
use Qubus\Dbal\Schema;
use Qubus\Inheritance\TapObjectAware;
use SplFixedArray;
use Stringable;
use Traversable;

use function array_fill;
use function array_key_exists;
use function array_keys;
use function array_map;
use function array_merge;
use function array_push;
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
use function is_null;
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

class OrmBuilder implements IteratorAggregate, Stringable
{
    use TapObjectAware;
    
    // Operators
    public const OPERATOR_AND = ' AND ';
    public const OPERATOR_OR = ' OR ';

    // Directional filter
    public const ORDERBY_ASC = 'ASC';
    public const ORDERBY_DESC = 'DESC';

    // Joins
    public const JOIN_INNER = 'INNER';
    public const JOIN_OUTER = 'OUTER';
    public const JOIN_LEFT = 'LEFT';
    public const JOIN_RIGHT = 'RIGHT';
    public const JOIN_RIGHT_OUTER = 'RIGHT OUTER';
    public const JOIN_LEFT_OUTER = 'LEFT OUTER';

    public const EOL = "\n";
    public const TAB = "\t";
    public const EOL_TAB = "\n\t";

    protected ?Connection $connection = null;
    protected ?string $tableName = null;
    protected string $tableToken = '';
    protected string $tableAlias = '';
    protected bool $isSingle = false;
    protected ?PDOStatement $pdoStmt = null;
    protected array $selectFields = [];
    protected array $joinSources = [];
    protected ?int $limit = null;
    protected ?int $offset = null;
    protected array $orderBy = [];
    protected array $groupBy = [];
    protected array $whereParameters = [];
    protected array $whereConditions = [];
    protected string $andOrOperator = self::OPERATOR_AND;
    protected array $having = [];
    protected bool $wrapOpen = false;
    protected int $lastWrapPosition = 0;
    protected bool $isFluentQuery = true;
    protected bool $pdoExecuted = false;
    protected array $data = [];
    protected bool $debugSqlQuery = false;
    protected string $sqlQuery = '';
    protected array $sqlParameters = [];
    protected array $dirtyFields = [];
    protected array $referenceKeys = [];
    protected bool $joinOn = false;
    protected static array $references = [];
    protected ?string $tablePrefix = null;
    /** @var Schema $schema */
    protected Schema $schema;
    /** @var array $tableStructure */
    public array $tableStructure = [
        'primaryKeyname' => 'id',
        'foreignKeyname' => '%s_id',
    ];

    /**
     * Constructor & set the table structure
     *
     * @param Connection $connection Database connection.
     * @param string|null $tablePrefix Prefix of database tables.
     * @param string $primaryKeyName Structure: table primary. If its an array, it must be the structure
     * @param string $foreignKeyName Structure: table foreignKeyName.
     *                                       It can be like %s_id where %s is the table name
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

    public static function fromInstance(
        Connection $connection,
        string $table,
        string $primaryKeyName = 'id',
        ?string $tablePrefix = null
    ): self {
        return (new self($connection))
        ->setStructure(primaryKeyName: $primaryKeyName)
        ->setTablePrefix(tablePrefix: $tablePrefix)
        ->table(tableName: $table);
    }

    /**
     * Define the working table and create a new instance
     *
     * @param string $tableName Table name.
     * @param ?string $alias     The table alias name.
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

    public function getConnection(): Connection
    {
        return $this->connection;
    }

    /**
     * Return the name of the table.
     */
    public function getTableName(): string
    {
        return $this->tableName;
    }

    /**
     * Set the table alias.
     *
     * @param string $alias
     * @return OrmBuilder
     */
    public function setTableAlias(string $alias): self
    {
        $this->tableAlias = $alias;
        return $this;
    }

    /**
     * Get table Alias
     */
    public function getTableAlias(): string
    {
        return $this->tableAlias;
    }

    /**
     * @param string $primaryKeyName The primary key, ie: id
     * @param string $foreignKeyName The foreign key as a pattern: %s_id,
     *                               where %s will be substituted with the table name
     * @return OrmBuilder
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

    public function setTablePrefix(?string $tablePrefix = ''): self
    {
        $this->tablePrefix = $tablePrefix;
        return $this;
    }

    /**
     * Return the table prefix.
     */
    public function getTablePrefix(): ?string
    {
        return $this->tablePrefix;
    }

    /**
     * Return the table structure.
     *
     * @return array
     */
    public function getStructure(): array
    {
        return $this->tableStructure;
    }

    /**
     * Get the primary key name.
     *
     * @return string
     */
    public function getPrimaryKeyname(): string
    {
        return $this->formatKeyname(pattern: $this->tableStructure['primaryKeyname'], tablename: $this->tableName);
    }

    /**
     * Get foreign key name.
     *
     * @return string
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
     * To execute a raw query
     *
     * @param string $query
     * @param array $parameters
     * @param bool $returnAsPdoStmt - true, it will return the PDOStatement
     *                                       false, it will return $this, which can be used for chaining
     *                                              or access the properties of the results
     * @return OrmBuilder|PDOStatement
     */
    public function query(
        string $query,
        array $parameters = [],
        bool $returnAsPdoStmt = false
    ): static|PDOStatement {
        $this->sqlParameters = $parameters;
        $this->sqlQuery = $query;

        if ($this->debugSqlQuery) {
            return $this;
        } else {
            $this->pdoStmt = $this->connection->getPdo()->prepare(query: $query);
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
     * To find all rows and create their instances
     * Use the query builder to build the where clause or $this->query with select
     * If a callback function is provided, the 1st arg must accept the rows results
     *
     * $this->find(function($rows){
     *   // do more stuff here...
     * });
     *
     * @param callable|null $callback Run a function on the returned rows
     * @return bool|SplFixedArray|string|ArrayIterator|InternalIterator|array
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
     * Return one row
     *
     * @param int|string|null $id Use to fetch by primary key.
     * @return OrmBuilder|false
     */
    public function findOne(int|string $id = null): self|bool
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
     * Create an instance from the given row (an associative
     * array of data fetched from the database).
     *
     * @param array $data
     * @return OrmBuilder
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
     * Create the select clause.
     *
     * @param mixed $columns The column(s) to select. Can be string or array of fields.
     * @param string|null $alias An alias to the column.
     * @return OrmBuilder
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
     * Add where condition, more calls appends with AND.
     *
     * @param mixed $condition condition possibly containing ? or :name
     * @param mixed $parameters array accepted by PDOStatement::execute or a scalar value.
     * @return OrmBuilder
     */
    public function where(mixed $condition, mixed $parameters = null): self
    {
        $this->isFluentQuery = true;

        // By default, the andOrOperator and wrap operator is AND,
        if ($this->wrapOpen || ! $this->andOrOperator) {
            $this->and__();
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
        } elseif (is_array(value: $parameters)) { // where("column", [1, 2]) => column IN (?,?)
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
        $this->and__();

        return $this;
    }

    /**
     * Create an AND operator in the where clause
     *
     * @return OrmBuilder
     */
    public function and__(): self
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
     * Create an OR operator in the where clause
     *
     * @return OrmBuilder
     */
    public function or__(): self
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
     * To group multiple where clauses together.
     *
     * @return OrmBuilder
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

        array_push($this->whereConditions, ')');
        $this->lastWrapPosition = count($this->whereConditions);

        return $this;
    }

    /**
     * Where Primary key
     *
     * @param int|string $id
     * @return OrmBuilder
     */
    public function wherePK(int|string $id): self
    {
        return $this->where(condition: $this->getPrimaryKeyname(), parameters: $id);
    }

    /**
     * WHERE $columName != $value
     *
     * @param string $columnName
     * @param mixed $value
     * @return OrmBuilder
     */
    public function whereNot(string $columnName, mixed $value): self
    {
        return $this->where(condition: "$columnName != ?", parameters: $value);
    }

    /**
     * WHERE $columName LIKE $value
     *
     * @param string $columnName
     * @param mixed $value
     * @return OrmBuilder
     */
    public function whereLike(string $columnName, mixed $value): self
    {
        return $this->where(condition: "$columnName LIKE ?", parameters: $value);
    }

    /**
     * WHERE $columName NOT LIKE $value
     *
     * @param string $columnName
     * @param mixed $value
     * @return OrmBuilder
     */
    public function whereNotLike(string $columnName, mixed $value): self
    {
        return $this->where(condition: "$columnName NOT LIKE ?", parameters: $value);
    }

    /**
     * WHERE $columName > $value
     *
     * @param string $columnName
     * @param mixed $value
     * @return OrmBuilder
     */
    public function whereGt(string $columnName, mixed $value): self
    {
        return $this->where(condition: "$columnName > ?", parameters: $value);
    }

    /**
     * WHERE $columName >= $value
     *
     * @param string $columnName
     * @param mixed $value
     * @return OrmBuilder
     */
    public function whereGte(string $columnName, mixed $value): self
    {
        return $this->where(condition: "$columnName >= ?", parameters: $value);
    }

    /**
     * WHERE $columName < $value
     *
     * @param string $columnName
     * @param mixed $value
     * @return OrmBuilder
     */
    public function whereLt(string $columnName, mixed $value): self
    {
        return $this->where(condition: "$columnName < ?", parameters: $value);
    }

    /**
     * WHERE $columName <= $value
     *
     * @param string $columnName
     * @param mixed $value
     * @return OrmBuilder
     */
    public function whereLte(string $columnName, mixed $value): self
    {
        return $this->where(condition: "$columnName <= ?", parameters: $value);
    }

    /**
     * WHERE $columName IN (?,?,?,...)
     *
     * @param string $columnName
     * @param array $values
     * @return OrmBuilder
     */
    public function whereIn(string $columnName, array $values): self
    {
        return $this->where(condition: $columnName, parameters: $values);
    }

    /**
     * WHERE $columName NOT IN (?,?,?,...)
     *
     * @param string $columnName
     * @param array $values
     * @return OrmBuilder
     */
    public function whereNotIn(string $columnName, array $values): self
    {
        $placeholders = $this->makePlaceholders(numberOfPlaceholders: count($values));

        return $this->where(condition: "({$columnName} NOT IN ({$placeholders}))", parameters: $values);
    }

    /**
     * WHERE $columName IS NULL
     *
     * @param string $columnName
     * @return OrmBuilder
     */
    public function whereNull(string $columnName): self
    {
        return $this->where(condition: "({$columnName} IS NULL)");
    }

    /**
     * WHERE $columName IS NOT NULL
     *
     * @param string $columnName
     * @return OrmBuilder
     */
    public function whereNotNull(string $columnName): self
    {
        return $this->where(condition: "({$columnName} IS NOT NULL)");
    }

    public function having($statement, $operator = self::OPERATOR_AND): self
    {
        $this->isFluentQuery = true;
        $this->having[] = [
            'STATEMENT' => $statement,
            'OPERATOR' => $operator,
        ];
        return $this;
    }

    /**
     * ORDER BY $columnName (ASC | DESC)
     *
     * @param  string   $columnName - The name of the colum or an expression
     * @param  string   $ordering   (DESC | ASC)
     * @return OrmBuilder
     */
    public function orderBy(string $columnName, string $ordering = ''): self
    {
        $this->isFluentQuery = true;
        $this->orderBy[] = "{$columnName} {$ordering}";
        return $this;
    }

    /**
     * GROUP BY $columnName
     *
     * @param string $columnName
     * @return OrmBuilder
     */
    public function groupBy(string $columnName): self
    {
        $this->isFluentQuery = true;
        $this->groupBy[] = $columnName;
        return $this;
    }

    /**
     * LIMIT $limit
     *
     * @param int|null $limit
     * @return OrmBuilder|int|null
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
     * OFFSET $offset
     *
     * @param int|null $offset
     * @return OrmBuilder|int|null
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

    /* ------------------------------------------------------------------------------
      JOIN
     * ----------------------------------------------------------------------------- */

    /**
     * Build a join
     *
     * @param string $tableName
     * @param string $constraint -> id = profile.user_id
     * @param string $tableAlias - The alias of the table name
     * @param string $joinOperator - LEFT | INNER | etc...
     * @return OrmBuilder
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
        $join .= " {$tableName} " . $tableAlias ? "AS {$tableAlias} " : '';
        $join .= self::EOL_TAB . self::TAB;
        $join .= "ON ({$constraint})";
        $join .= self::EOL;
        $this->joinSources[] = $join;
        return $this;
    }

    /**
     * An alias to join by using a QueryBuilder instance.
     * The QueryBuilder instance may have select and where statement for the ON clause
     *
     * @param OrmBuilder $query
     * @param string $joinOperator
     * @return OrmBuilder
     */
    public function on(OrmBuilder $query, string $joinOperator = self::JOIN_LEFT): self
    {
        $this->joinOn = true;
        $constraint = str_replace(
            search: '%join.',
            replace: $this->getTableAlias() . '.',
            subject: $query->getJoinOnString()
        );

        $this->select(
            columns: array_map(callback: function ($row) {
                return str_replace('%join.', $this->getTableAlias() . '.', $row);
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
     * The associated schema instance.
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
     * Return the select fields as array.
     *
     * @return array
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
                if ($whereCondition && $lastCondition != "(" && ! preg_match(pattern: "/\)\s+(OR|AND)\s+$/i", subject: $whereCondition)) {
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
            $columns[$column] = strpos(haystack: $column, needle: '.') === false ? "%this.{$column}" : $column;
        }
        $stmt = str_replace(
            search: array_keys(array: $columns),
            replace: array_values($columns),
            subject: $whereCondition
        );
        return $this->formatColumnName(column: $stmt);
    }

    /**
     * Create the JOIN ... ON string when there is a join. It will be called by on().
     *
     * @return string
     */
    public function getJoinOnString(): string
    {
        $where = $this->getWhereString();

        $params = $this->whereParameters;
        return preg_replace_callback(pattern: '/\?/', callback: function ($match) use (&$params) {
            $arg = array_shift($params);
            if (is_numeric(value: $arg)) {
                return $arg;
            } elseif (strpos(haystack: $arg, needle: '%') !== false) {
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
     * Detect if its a single row instance and reset it to PK.
     *
     * @return OrmBuilder
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
     * @return OrmBuilder
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
     * Retrieves the ID of the last record inserted.
     *
     * @param string|null $pk
     * @return string|false
     */
    public function lastInsertId(string|null $pk = null): string|false
    {
        if ($this->connection->getPdo()->getAttribute(attribute: PDO::ATTR_DRIVER_NAME) === 'pgsql') {
            $pk = sprintf('%s_%s_seq', $this->getTableName(), $pk);
        }
        return $this->connection->getPdo()->lastInsertId(name: $pk);
    }

    /**
     * Insert new rows
     * $data can be 2 dimensional to add a bulk insert
     * If a single row is inserted, it will return it's row instance
     *
     * @param  array    $data - data to populate
     * @return OrmBuilder|int
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
     * Update entries
     * Use the query builder to create the where clause.
     *
     * @param array|null $data the data to update
     * @return OrmBuilder|int|false
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
     * Delete rows.
     *
     * Use the query builder to create the where clause.
     *
     * @param bool $deleteAll When there is no where condition, setting to true will delete all.
     * @return OrmBuilder|int|false
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
        return $this->connection->getPdo()->beginTransaction();
    }

    /**
     * Checks if inside transaction.
     *
     * @return bool
     */
    public function inTransaction(): bool
    {
        return $this->connection->getPdo()->inTransaction();
    }

    /**
     * Commits a transaction
     *
     * @return bool
     */
    public function commit(): bool
    {
        return $this->connection->getPdo()->commit();
    }

    /**
     * Rolls back a transaction.
     *
     * @return bool
     */
    public function rollBack(): bool
    {
        return $this->connection->getPdo()->rollBack();
    }

    /* ------------------------------------------------------------------------------
      Set & Save
     * ----------------------------------------------------------------------------- */

    /**
     * To set data for update or insert
     * $key can be an array for mass set
     *
     * @param  mixed    $key
     * @param mixed|null $value
     * @return OrmBuilder
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
     * Save, a shortcut to update() or insert().
     *
     * @return OrmBuilder|int|bool|static
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
     * Return the aggregate count of column
     *
     * @param string|null $column - the column name
     * @return float|int
     */
    public function count(?string $column = null): float|int
    {
        if (! $column) {
            $column = $this->getPrimaryKeyname();
        }
        return $this->aggregate(fn: "COUNT({$this->prepareColumn(column: $column)})");
    }

    /**
     * Return the aggregate max count of column
     *
     * @param string $column - the column name
     * @return float|int
     */
    public function max(string $column): float|int
    {
        return $this->aggregate(fn: "MAX({$this->prepareColumn(column: $column)})");
    }

    /**
     * Return the aggregate min count of column
     *
     * @param string $column - the column name
     * @return float|int
     */
    public function min(string $column): float|int
    {
        return $this->aggregate(fn: "MIN({$this->prepareColumn(column: $column)})");
    }

    /**
     * Return the aggregate sum count of column
     *
     * @param string $column - the column name
     * @return float|int
     */
    public function sum(string $column): float|int
    {
        return $this->aggregate(fn: "SUM({$this->prepareColumn(column: $column)})");
    }

    /**
     * Return the aggregate average count of column
     *
     * @param string $column - the column name
     * @return float|int
     */
    public function avg(string $column): float|int
    {
        return $this->aggregate(fn: "AVG({$this->prepareColumn(column: $column)})");
    }

    /**
     * @param string $fn - The function to use for the aggregation
     * @return float|int
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
     * Get the a key
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
     * @return OrmBuilder
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
        return (new DateTime(datetime: $datetime ?: 'now'))->format(format: 'Y-m-d H:i:s');
    }

    // QueryBuilder Debugger

    /**
     * To debug the query. It will not execute it but instead using debugSqlQuery()
     * and getSqlParameters to get the data
     *
     * @param bool $bool
     * @return OrmBuilder
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
        return $this->isSingle ? $this->getPK() : $this->tableName;
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
        if (strpos(haystack: $column, needle: '.') === false
            && strpos(haystack: strtoupper(string: $column), needle: 'NULL') === false) {
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
        return str_replace(search: '%this.', replace: $this->getTableAlias() . '.', subject: $column);
    }
}
