# dbtools

[![PkgGoDev](https://pkg.go.dev/badge/github.com/arsham/dbtools)](https://pkg.go.dev/github.com/arsham/dbtools)
![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/arsham/dbtools)
[![Build Status](https://github.com/arsham/dbtools/actions/workflows/go.yml/badge.svg)](https://github.com/arsham/dbtools/actions/workflows/go.yml)
[![Coverage Status](https://codecov.io/gh/arsham/dbtools/branch/master/graph/badge.svg)](https://codecov.io/gh/arsham/dbtools)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Go Report Card](https://goreportcard.com/badge/github.com/arsham/dbtools)](https://goreportcard.com/report/github.com/arsham/dbtools)

This library contains concurrent safe helpers for retrying transactions until
they succeed and handles errors in a developer friendly way. There are helpers
for using with [go-sqlmock][go-sqlmock] in tests. There is also a `Mocha`
inspired reporter for [spec BDD library][spec].

This library supports `Go >= 1.22`. To use this library use this import path:

```
github.com/arsham/dbtools/v4
```

For Go >= 1.20 support use the v3:

```
github.com/arsham/dbtools/v3
```

For older Go's support use the v2:

```
github.com/arsham/dbtools/v2
```

1. [PGX Transaction](#pgx-transaction)
   - [Common Patterns](#common-patterns)
2. [SQLMock Helpers](#sqlmock-helpers)
   - [ValueRecorder](#valuerecorder)
   - [OkValue](#okvalue)
3. [Spec Reports](#spec-reports)
   - [Usage](#usage)
4. [Development](#development)
5. [License](#license)

## PGX Transaction

The `PGX` struct helps reducing the amount of code you put in the logic by
taking care of errors. For example instead of writing:

```go
tx, err := db.Begin(ctx)
if err != nil {
	return errors.Wrap(err, "starting transaction")
}
err := firstQueryCall(tx)
if err != nil {
	e := errors.Wrap(tx.Rollback(ctx), "rolling back transaction")
	return multierror.Append(err, e).ErrorOrNil()
}
err := secondQueryCall(tx)
if err != nil {
	e := errors.Wrap(tx.Rollback(ctx), "rolling back transaction")
	return multierror.Append(err, e).ErrorOrNil()
}
err := thirdQueryCall(tx)
if err != nil {
	e := errors.Wrap(tx.Rollback(ctx), "rolling back transaction")
	return multierror.Append(err, e).ErrorOrNil()
}

return errors.Wrap(tx.Commit(ctx), "committing transaction")

```

You will write:

```go
// for using with pgx connections:
p, err := dbtools.NewPGX(conn)
// handle the error!
return p.Transaction(ctx, firstQueryCall, secondQueryCall, thirdQueryCall)
```

At any point any of the callback functions return an error, the transaction is
rolled-back, after the given delay the operation is retried in a new
transaction.

You may set the retry count, delays, and the delay method by passing
`dbtools.ConfigFunc` helpers to the constructor. If you don't pass any config,
the `Transaction` method will run only once.

You can prematurely stop retrying by returning a `*retry.StopError` error:

```go
err = p.Transaction(ctx, func(tx pgx.Tx) error {
	_, err := tx.Exec(ctx, query)
	return &retry.StopError{Err: err}
})
```

See [retry][retry] library for more information.

The callback functions should be of `func(pgx.Tx) error` type. To try up to 20
time until your queries succeed:

```go
// conn is a *pgxpool.Pool instance
p, err := dbtools.NewPGX(conn, dbtools.Retry(20))
// handle the error
err = p.Transaction(ctx, func(tx pgx.Tx) error {
	// use tx to run your queries
	return someErr
}, func(tx pgx.Tx) error {
	return someErr
}, func(tx pgx.Tx) error {
	return someErr
	// add more callbacks if required.
})
// handle the error!
```

### Common Patterns

Stop retrying when the row is not found:

```go
err := retrier.Do(func() error {
	const query = `SELECT foo FROM bar WHERE id = $1::int`
	err := conn.QueryRow(ctx, query, msgID).Scan(&foo)
	if errors.Is(err, pgx.ErrNoRows) {
		return &retry.StopError{Err: ErrFooNotFound}
	}
	return errors.Wrap(err, "quering database")
})
```

Stop retrying when there are integrity errors:

```go
// integrityCheckErr returns a *retry.StopError wrapping the err with the msg
// if the query causes integrity constraint violation error. You should use
// this check to stop the retry mechanism, otherwise the transaction repeats.
func integrityCheckErr(err error, msg string) error {
    var v *pgconn.PgError
    if errors.As(err, &v) && isIntegrityConstraintViolation(v.Code) {
        return &retry.StopError{Err: errors.Wrap(err, msg)}
    }
    return errors.Wrap(err, msg)
}

func isIntegrityConstraintViolation(code string) bool {
    switch code {
    case pgerrcode.IntegrityConstraintViolation,
        pgerrcode.RestrictViolation,
        pgerrcode.NotNullViolation,
        pgerrcode.ForeignKeyViolation,
        pgerrcode.CheckViolation,
        pgerrcode.ExclusionViolation:
        return true
    }
    return false
}

err := p.Transaction(ctx, func(tx pgx.Tx) error {
    const query = `INSERT INTO foo (bar) VALUES ($1::text)`
    err := tx.Exec(ctx, query, name)
    return integrityCheckErr(err, "creating new record")
}, func(tx pgx.Tx) error {
    const query = `UPDATE baz SET updated_at=NOW()::timestamptz WHERE id = $1::int`
    _, err := tx.Exec(ctx, query, msgID)
    return err
})
```

This is not a part of the `dbtools` library, but it deserves a mention. Here is
a common pattern for querying for multiple rows:

```go
result := make([]Result, 0, expectedTotal)
err := retrier.Do(func() error {
	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return errors.Wrap(err, "making query")
	}

	defer rows.Close()

	// make sure you reset the slice, otherwise in the next retry it adds the
	// same data to the slice again.
	result = result[:0]
	for rows.Next() {
		var doc Result
		err := rows.Scan(&doc.A, &doc.B)
		if err != nil {
			return errors.Wrap(err, "scanning rows")
		}
		result = append(result, doc)
	}

	return errors.Wrap(rows.Err(), "row error")
})
// handle the error!
```

## SQLMock Helpers

There a couple of helpers for using with [go-sqlmock][go-sqlmock] test cases for
cases that values are random but it is important to check the values passed in
queries.

### ValueRecorder

If you have an value and use it in multiple queries, and you want to
make sure the queries are passed with correct values, you can use the
`ValueRecorder`. For example UUIDs, time and random values.

For instance if the first query generates a random number but it is essential to
use the same value on next queries:

```go
import "database/sql"

func TestFoo(t *testing.T) {
	// ...
	// assume num has been generated randomly
	num := 666
	_, err := tx.ExecContext(ctx, "INSERT INTO life (value) VALUE ($1)", num)
	// error check
	_, err = tx.ExecContext(ctx, "INSERT INTO reality (value) VALUE ($1)", num)
	// error check
	_, err = tx.ExecContext(ctx, "INSERT INTO everywhere (value) VALUE ($1)", num)
	// error check
}
```

Your tests can be checked easily like this:

```go
import (
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/arsham/dbtools/v3/dbtesting"
)

func TestFoo(t *testing.T) {
	// ...
	rec := dbtesting.NewValueRecorder()
	mock.ExpectExec("INSERT INTO life .+").
		WithArgs(rec.Record("truth")).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO reality .+").
		WithArgs(rec.For("truth")).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO everywhere .+").
		WithArgs(rec.For("truth")).
		WillReturnResult(sqlmock.NewResult(1, 1))
}
```

Recorded values can be retrieved by casting to their types:

```go
rec.Value("true").(string)
```

There are two rules for using the `ValueRecorder`:

1. You can only record for a value once.
2. You should record a value before you call `For` or `Value`.

It will panic if these requirements are not met.

### OkValue

If you are only interested in checking some arguments passed to the Exec/Query
functions and you don't want to check everything (maybe because thy are not
relevant to the current test), you can use `OkValue`.

```go
import (
    "github.com/arsham/dbtools/v3/dbtesting"
    "github.com/DATA-DOG/go-sqlmock"
)

ok := dbtesting.OkValue
mock.ExpectExec("INSERT INTO life .+").
    WithArgs(
        ok,
        ok,
        ok,
        "important value"
        ok,
        ok,
        ok,
    )
```

## Spec Reports

`Mocha` is a reporter for printing Mocha inspired reports when using
[spec BDD library][spec].

### Usage

```go
import "github.com/arsham/dbtools/v3/dbtesting"

func TestFoo(t *testing.T) {
	spec.Run(t, "Foo", func(t *testing.T, when spec.G, it spec.S) {
		// ...
	}, spec.Report(&dbtesting.Mocha{}))
}

```

You can set an `io.Writer` to `Mocha.Out` to redirect the output, otherwise it
prints to the `os.Stdout`.

## Development

Run the `tests` target for watching file changes and running tests:

```bash
make tests
```

You can pass flags as such:

```bash
make tests flags="-race -v -count=5"
```

You need to run the `dependencies` target for installing [reflex][reflex] task
runner:

```bash
make dependencies
```

## License

Use of this source code is governed by the Apache 2.0 license. License can be
found in the [LICENSE](./LICENSE) file.

[retry]: https://github.com/arsham/retry
[pgx]: https://github.com/jackc/pgx
[go-sqlmock]: https://github.com/DATA-DOG/go-sqlmock
[spec]: https://github.com/sclevine/spec
[reflex]: https://github.com/cespare/reflex

<!--
vim: foldlevel=1
-->
