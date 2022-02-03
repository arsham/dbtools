# dbtools

[![PkgGoDev](https://pkg.go.dev/badge/github.com/arsham/dbtools)](https://pkg.go.dev/github.com/arsham/dbtools)
![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/arsham/dbtools)
[![Build Status](https://github.com/arsham/dbtools/actions/workflows/go.yml/badge.svg)](https://github.com/arsham/dbtools/actions/workflows/go.yml)
[![Coverage Status](https://codecov.io/gh/arsham/dbtools/branch/master/graph/badge.svg)](https://codecov.io/gh/arsham/dbtools)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Go Report Card](https://goreportcard.com/badge/github.com/arsham/dbtools)](https://goreportcard.com/report/github.com/arsham/dbtools)

This library contains goroutine safe helpers for retrying transactions until
they succeed and handles errors in a developer friendly way. There are helpers
for using with [go-sqlmock][go-sqlmock] in tests. There is also a `Mocha`
inspired reporter for [spec BDD library][spec].

This library supports `Go >= 1.17`.

1. [Transaction](#transaction)
   - [PGX Pool](#pgx-pool)
   - [Standard Library](#standard-library)
2. [SQLMock Helpers](#sqlmock-helpers)
   - [ValueRecorder](#valuerecorder)
   - [OkValue](#okvalue)
3. [Spec Reports](#spec-reports)
   - [Usage](#usage)
4. [Development](#development)
5. [License](#license)

## Transaction

`Transaction` helps you reduce the amount of code you put in the logic by taking
care of errors. For example instead of writing:

```go
tx, err := db.Begin()
if err != nil {
    return errors.Wrap(err, "starting transaction")
}
err := firstQueryCall(tx)
if err != nil {
    e := errors.Wrap(tx.Rollback(), "rolling back transaction")
    return multierror.Append(err, e).ErrorOrNil()
}
err := secondQueryCall(tx)
if err != nil {
    e := errors.Wrap(tx.Rollback(), "rolling back transaction")
    return multierror.Append(err, e).ErrorOrNil()
}
err := thirdQueryCall(tx)
if err != nil {
    e := errors.Wrap(tx.Rollback(), "rolling back transaction")
    return multierror.Append(err, e).ErrorOrNil()
}

return errors.Wrap(tx.Commit(), "committing transaction")

```

You will write:

```go
// for using with pgx connections:
tr, err := dbtools.NewTransaction(conn)
// handle error, and reuse tr
return tr.PGX(ctx, firstQueryCall, secondQueryCall, thirdQueryCall)

// or to use with stdlib sql.DB:
tr, err := dbtools.NewTransaction(conn)
// handle error, and reuse tr
return tr.DB(ctx, firstQueryCall, secondQueryCall, thirdQueryCall)
```

At any point a transaction function returns an error, the whole transaction is
started over.

You may set the retry count, delays, and the delay method by passing
`dbtools.ConfigFunc` functions to the constructor. If you don't pass any
config, `PGX` and `DB` methods will run only once.

You can prematurely stop retrying by returning a `retry.StopError` error:

```go
err = tr.PGX(ctx, func(tx pgx.Tx) error {
    _, err := tx.Exec(ctx, query)
    return retry.StopError{Err: err}
})
```

See [retry][retry] library for more information.

### PGX Pool

Your transaction functions should be of `func(pgx.Tx) error` type. To try up to
20 time until your queries succeed:

```go
// conn is a *sql.DB instance
tr, err := dbtools.NewTransaction(conn, dbtools.Retry(20))
// handle error
err = tr.PGX(ctx, func(tx pgx.Tx) error {
    // use tx to run your queries
    return err
}, func(tx pgx.Tx) error {
    return err
})
// handle error
```

### Standard Library

Your transaction functions should be of `func(dbtools.Tx) error` type. To try up to
20 time until your queries succeed:

```go
// conn is a *pgxpool.Pool instance
tr, err := dbtools.NewTransaction(conn, dbtools.Retry(20))
// handle error
err = tr.DB(ctx, func(tx dbtools.Tx) error {
    // use tx to run your queries
    return err
}, func(tx dbtools.Tx) error {
    return err
})
// handle error
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
    "github.com/arsham/dbtools/dbtesting"
    "github.com/DATA-DOG/go-sqlmock"
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
    "github.com/arsham/dbtools/dbtesting"
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
import "github.com/arsham/dbtools/dbtesting"

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
