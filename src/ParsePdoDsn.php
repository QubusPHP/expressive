<?php

declare(strict_types=1);

namespace Qubus\Expressive;

use InvalidArgumentException;

final readonly class ParsePdoDsn
{
    private const array SQLITE_DRIVERS = ['sqlite', 'sqlite2', 'sqlite3'];

    public function __construct(
        private string $driver,
        private array $parameters = [],
        private ?string $path = null,
    ) {
    }

    public static function fromString(string $dsn): self
    {
        return str_contains($dsn, '://')
        ? self::fromUri($dsn)
        : self::fromPdoDsn($dsn);
    }

    private static function fromPdoDsn(string $dsn): self
    {
        if (! str_contains($dsn, ':')) {
            throw new InvalidArgumentException(sprintf('Invalid DSN string: "%s"', $dsn));
        }

        [$driver, $config] = explode(':', $dsn, 2);

        $driver = trim($driver);
        $config = trim($config);

        if ($driver === '') {
            throw new InvalidArgumentException('DSN driver cannot be empty.');
        }

        if (self::isSqliteDriver($driver)) {
            return new self(
                driver: $driver,
                path: $config,
            );
        }

        $parameters = [];

        foreach (explode(';', $config) as $segment) {
            if ($segment === '') {
                continue;
            }

            [$key, $value] = array_pad(explode('=', $segment, 2), 2, null);

            $key = trim((string) $key);

            if ($key === '' || $value === null) {
                continue;
            }

            $parameters[$key] = trim($value);
        }

        return new self(
            driver: $driver,
            parameters: $parameters,
        );
    }

    private static function fromUri(string $uri): self
    {
        $parts = parse_url($uri);

        if ($parts === false || empty($parts['scheme'])) {
            throw new InvalidArgumentException(sprintf('Invalid URI DSN string: "%s"', $uri));
        }

        $driver = strtolower($parts['scheme']);

        $parameters = [];

        if (isset($parts['host'])) {
            $parameters['host'] = $parts['host'];
        }

        if (isset($parts['port'])) {
            $parameters['port'] = (string) $parts['port'];
        }

        if (isset($parts['user'])) {
            $parameters['user'] = rawurldecode($parts['user']);
        }

        if (isset($parts['pass'])) {
            $parameters['password'] = rawurldecode($parts['pass']);
        }

        if (! empty($parts['path'])) {
            $parameters['dbname'] = ltrim($parts['path'], '/');
        }

        if (! empty($parts['query'])) {
            parse_str($parts['query'], $query);

            foreach ($query as $key => $value) {
                if (is_scalar($value)) {
                    $parameters[(string) $key] = (string) $value;
                }
            }
        }

        return new self(
            driver: $driver,
            parameters: $parameters,
        );
    }

    public function driver(): string
    {
        return $this->driver;
    }

    public function isSqlite(): bool
    {
        return self::isSqliteDriver($this->driver);
    }

    public function host(): ?string
    {
        return $this->get('host');
    }

    public function port(): ?int
    {
        $port = $this->get('port');

        return $port !== null ? (int) $port : null;
    }

    public function database(): ?string
    {
        return $this->isSqlite()
        ? $this->path
        : $this->get('dbname');
    }

    public function path(): ?string
    {
        return $this->path;
    }

    public function username(): ?string
    {
        return $this->get('user');
    }

    public function password(): ?string
    {
        return $this->get('password');
    }

    public function charset(): ?string
    {
        return $this->get('charset');
    }

    public function get(string $key, mixed $default = null): mixed
    {
        return $this->parameters[$key] ?? $default;
    }

    public function has(string $key): bool
    {
        return array_key_exists($key, $this->parameters);
    }

    public function parameters(): array
    {
        return $this->parameters;
    }

    public function toArray(): array
    {
        return [
            'driver' => $this->driver,
            ...($this->isSqlite()
                    ? ['path' => $this->path]
                    : $this->parameters),
        ];
    }

    public function toPdoDsn(): string
    {
        if ($this->isSqlite()) {
            return "{$this->driver}:{$this->path}";
        }

        $segments = [];

        foreach ($this->parameters as $key => $value) {
            if (in_array($key, ['user', 'password'], true)) {
                continue;
            }

            $segments[] = "{$key}={$value}";
        }

        return "{$this->driver}:" . implode(';', $segments);
    }

    public function toString(): string
    {
        return $this->toPdoDsn();
    }

    public function __toString(): string
    {
        return $this->toString();
    }

    private static function isSqliteDriver(string $driver): bool
    {
        return in_array(strtolower($driver), self::SQLITE_DRIVERS, true);
    }
}
