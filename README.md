# dbtesting

Utility for using with go-sqlmock library.

This library can be used in the [go-sqlmock][go-sqlmock] test cases for cases
that values are random but it is important to check the values passed in
queries.

## ValueRecorder

If you generate a UUID and use it in multiple queries and you want to make sure
the queries are passed with correct IDs. For instance if in your code you have:

```go
import "database/sql"

// ...

// assume num has been generated randomly
num := 666
_, err := tx.ExecContext(ctx, "INSERT INTO life (value) VALUE ($1)", num)
// error check
_, err := tx.ExecContext(ctx, "INSERT INTO reality (value) VALUE ($1)", num)
// error check
_, err := tx.ExecContext(ctx, "INSERT INTO everywhere (value) VALUE ($1)", num)
// error check
```

Your tests can be checked easily like this:
```go
import (
    "github.com/arsham/dbtesting"
    "github.com/DATA-DOG/go-sqlmock"
    // ...
)

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
```

## OkValue

When you are only interested in checking some arguments passed to the Exec/Query
functions and you don't want to check everything (maybe because thy are not
relevant to the current test), you can use the `OkValue`.

```go
import (
    "github.com/arsham/dbtesting"
    "github.com/DATA-DOG/go-sqlmock"
    // ...
)

mock.ExpectExec("INSERT INTO life .+").
    WithArgs(
        dbtesting.OkValue,
        dbtesting.OkValue,
        dbtesting.OkValue,
        "import value"
        dbtesting.OkValue,
        dbtesting.OkValue,
        dbtesting.OkValue,
    )
```

## LICENSE

Use of this source code is governed by the Apache 2.0 license. License can be
found in the [LICENSE](./LICENSE) file.

[go-sqlmock]: github.com/DATA-DOG/go-sqlmock
