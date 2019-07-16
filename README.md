# dbtesting

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![GoDoc](https://godoc.org/github.com/arsham/dbtesting?status.svg)](http://godoc.org/github.com/arsham/dbtesting)
[![Build Status](https://travis-ci.org/arsham/dbtesting.svg?branch=master)](https://travis-ci.org/arsham/dbtesting)
[![Coverage Status](https://codecov.io/gh/arsham/dbtesting/branch/master/graph/badge.svg)](https://codecov.io/gh/arsham/dbtesting)

This library has a few helpers for using in tests.

1. [Spec Reports](#spec-reports)
    * [Usage](#usage)
2. [SQLMock Helpers](#sqlmock-helpers)
    * [ValueRecorder](#valuerecorder)
    * [OkValue](#okvalue)
3. [Testing](#testing)
4. [License](#license)

## Spec Reports

`Mocha` is a reporter for printing Mocha inspired reports when using
[spec BDD library][spec].

### Usage

```go
import "github.com/arsham/dbtesting"

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
    "github.com/arsham/dbtesting"
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
    "github.com/arsham/dbtesting"
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
or for with `-race` flag:
```bash
make test_race
```

If you don't have `reflex` installed, run the following once:
```bash
make third-party
```

## License

Use of this source code is governed by the Apache 2.0 license. License can be
found in the [LICENSE](./LICENSE) file.

[go-sqlmock]: https://github.com/DATA-DOG/go-sqlmock
[spec]: https://github.com/sclevine/spec
