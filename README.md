# dbtools

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![GoDoc](https://godoc.org/github.com/arsham/dbtools?status.svg)](http://godoc.org/github.com/arsham/dbtools)
[![Build Status](https://travis-ci.org/arsham/dbtools.svg?branch=master)](https://travis-ci.org/arsham/dbtools)
[![Coverage Status](https://codecov.io/gh/arsham/dbtools/branch/master/graph/badge.svg)](https://codecov.io/gh/arsham/dbtools)

This library has a few helpers for using in production code and
[go-sqlmock][go-sqlmock] tests. There is also a `Mocha` inspired reporter for
[spec BDD library][spec].

1. [Transaction](#transaction)
    * [WithTransaction](#withtransaction)
    * [Retry](#retry)
    * [RetryTransaction](#retrytransaction)
2. [Spec Reports](#spec-reports)
    * [Usage](#usage)
3. [SQLMock Helpers](#sqlmock-helpers)
    * [ValueRecorder](#valuerecorder)
    * [OkValue](#okvalue)
4. [Testing](#testing)
5. [License](#license)

## Transaction

### WithTransaction

`WithTransaction` helps you reduce the amount of code you put in the logic by
taking care of errors. For example instead of writing:

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

You can write:
```go
return dbtools.WithTransaction(db, firstQueryCall, secondQueryCall, thirdQueryCall)
```

Function types should be of `func(*sql.Tx) error`.

### Retry

`Retry` calls your function, and if it errors it calls it again with a delay.
Every time the function returns an error it increases the delay. Eventually it
returns the last error or nil if one call is successful.

You can use this function in non-database situations too.

```go
dbtools.Retry(10, time.Second. func(i int) error {
    logger.Debugf("%d iteration", i)
    return myFunctionCall()
})
```

### RetryTransaction

`RetryTransaction` is a combination of `WithTransaction` and `Retry`. It stops
the retry if the context is cancelled/done.

```go
err := dbtools.RetryTransaction(ctx, db, 10, time.Millisecond * 10,
    firstQueryCall,
    secondQueryCall,
    thirdQueryCall,
)
// error check
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

## Testing

To run the tests:

```bash
make
```
`test_race` target runs tests with `-race` flag. `third-party` installs
[reflex][reflex] task runner.

## License

Use of this source code is governed by the Apache 2.0 license. License can be
found in the [LICENSE](./LICENSE) file.

[go-sqlmock]: https://github.com/DATA-DOG/go-sqlmock
[spec]: https://github.com/sclevine/spec
[reflex]: https://github.com/cespare/reflex
