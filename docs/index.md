# Expressive

Expressive is a PDO-based database toolkit for PHP 8.4 and later. It provides connection helpers, a fluent query
builder, schema and migration support, Active Record, and DataMapper.

Start with the component that matches the application boundary:

- [Connections](connections.md) explains supported drivers, DSNs, parameter binding, transactions, and connection
  isolation.
- [QueryBuilder](query-builder.md) documents fluent selects, writes, filters, aggregates, joins, and direct SQL.
- [DataMapper](data-mapper.md) maps typed persistence-independent entities.
- [Active Record](active-record.md) combines row data and persistence behavior in models.

Query values should always be passed as bound parameters. SQL identifiers and expressions must come from trusted
application code or an explicit allowlist.

