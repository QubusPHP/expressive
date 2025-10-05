<?php

declare(strict_types=1);

namespace Qubus\Expressive;

class Expression
{
    /** @var  mixed  $value  the raw expression */
    protected mixed $value;

    /**
     * @param mixed $value expression value
     */
    public function __construct(mixed $value)
    {
        $this->value = $value;
    }

    /**
     * Get the expression value as a string.
     *
     *     $sql = $expression->value();
     *
     * @return  string
     */
    public function value(): string
    {
        return (string) $this->value;
    }

    /**
     * Return the value of the expression as a string.
     *
     *     echo $expression;
     *
     * @return  string
     * @uses    Expression::value
     */
    public function __toString()
    {
        return $this->value();
    }
}
