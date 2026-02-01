<?php

declare(strict_types=1);

namespace Qubus\Expressive;

interface Join
{
    public const string JOIN_INNER = 'INNER';
    public const string JOIN_OUTER = 'OUTER';
    public const string JOIN_LEFT = 'LEFT';
    public const string JOIN_RIGHT = 'RIGHT';
    public const string JOIN_RIGHT_OUTER = 'RIGHT OUTER';
    public const string JOIN_LEFT_OUTER = 'LEFT OUTER';

    /**
     * Build a join
     *
     * @param string $tableName
     * @param string $constraint -> id = profile.user_id
     * @param string $tableAlias - The alias of the table name
     * @param string $joinOperator - LEFT | INNER | etc...
     * @return self
     */
    public function join(
        string $tableName,
        string $constraint,
        string $tableAlias = '',
        string $joinOperator = self::JOIN_LEFT
    ): self;

    /**
     * An alias to join by using a Database instance.
     * The Database instance may have select and where statement for the ON clause
     *
     * @param Database $query
     * @param string $joinOperator
     * @return self
     */
    public function on(Database $query, string $joinOperator = self::JOIN_LEFT): self;

    /**
     * Create the JOIN ... ON string when there is a join. It will be called by on().
     *
     * @return string
     */
    public function getJoinOnString(): string;
}
