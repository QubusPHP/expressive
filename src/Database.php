<?php

declare(strict_types=1);

namespace Qubus\Expressive;

use Closure;
use Exception;

interface Database extends Singleton, Table, Select, Where, Insert, Update, Set, Delete, Join, Aggregate
{
    /**
     * The associated schema instance.
     */
    public function schema(): Schema;

    /**
     * Run transactional queries.
     *
     * @param Closure $callback transaction callback
     * @throws Exception
     */
    public function transactional(Closure $callback, mixed $that = null, mixed $default = null): mixed;
}
