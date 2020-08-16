package dbtools_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/arsham/dbtools"
	"github.com/arsham/dbtools/mocks"
	"github.com/arsham/retry"
	"github.com/hashicorp/go-multierror"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/jackc/pgx/v4/stdlib"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestNewTransaction(t *testing.T) {
	t.Parallel()
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	tcs := map[string]struct {
		db      interface{}
		conf    []dbtools.ConfigFunc
		wantErr error
	}{
		"nil db":        {nil, nil, dbtools.ErrEmptyDatabase},
		"nil pgxpool":   {nil, nil, dbtools.ErrEmptyDatabase},
		"nil sql.DB":    {nil, nil, dbtools.ErrEmptyDatabase},
		"wrong db type": {"db", nil, dbtools.ErrEmptyDatabase},
		"db":            {&mocks.DB{}, nil, nil},
		"pool":          {&mocks.Pool{}, nil, nil},
		"sql.DB":        {db, nil, nil},
		"low attempts":  {db, []dbtools.ConfigFunc{dbtools.RetryCount(-1)}, nil},
		"delay method":  {db, []dbtools.ConfigFunc{dbtools.DelayMethod(retry.IncrementalDelay)}, nil},
	}
	for name, tc := range tcs {
		tc := tc
		t.Run(name, func(t *testing.T) {
			_, err := dbtools.NewTransaction(tc.db, tc.conf...)
			if tc.wantErr == nil {
				assert.NoError(t, err)
				return
			}
			assertInError(t, err, tc.wantErr)
		})
	}
}

func TestTransaction(t *testing.T) {
	t.Run("PGX", testTransactionPGX)
	t.Run("DB", testTransactionDB)
}

func testTransactionPGX(t *testing.T) {
	t.Run("NilDatabase", testTransactionPGXNilDatabase)
	t.Run("BeginError", testTransactionPGXBeginError)
	t.Run("CancelledContext", testTransactionPGXCancelledContext)
	t.Run("Panic", testTransactionPGXPanic)
	t.Run("AnError", testTransactionPGXAnError)
	t.Run("RollbackError", testTransactionPGXRollbackError)
	t.Run("CommitError", testTransactionPGXCommitError)
	t.Run("ShortStop", testTransactionPGXShortStop)
	t.Run("RetrySuccess", testTransactionPGXRetrySuccess)
	t.Run("MultipleFunctions", testTransactionPGXMultipleFunctions)
	t.Run("RealDatabase", testTransactionPGXRealDatabase)
}

func testTransactionPGXNilDatabase(t *testing.T) {
	t.Parallel()
	db := &mocks.DB{}
	defer db.AssertExpectations(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tr, err := dbtools.NewTransaction(db)
	require.NoError(t, err)

	err = tr.PGX(ctx, func(pgx.Tx) error {
		t.Error("didn't expect to receive this call")
		return nil
	})
	assertInError(t, err, dbtools.ErrEmptyDatabase)

	tr = &dbtools.Transaction{}
	err = tr.PGX(ctx, func(pgx.Tx) error {
		t.Error("didn't expect to receive this call")
		return nil
	})
	assertInError(t, err, dbtools.ErrEmptyDatabase)
}

func testTransactionPGXBeginError(t *testing.T) {
	t.Parallel()
	db := &mocks.Pool{}
	defer db.AssertExpectations(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	total := 3
	tr, err := dbtools.NewTransaction(db, dbtools.RetryCount(total))
	require.NoError(t, err)

	db.On("Begin", mock.Anything).
		Return(nil, assert.AnError).Times(total)

	err = tr.PGX(ctx, func(pgx.Tx) error {
		t.Error("didn't expect to receive this call")
		return nil
	})
	assertInError(t, err, assert.AnError)
}

func testTransactionPGXCancelledContext(t *testing.T) {
	t.Run("FirstFunction", testTransactionPGXCancelledContextFirstFunction)
	t.Run("SecondFunction", testTransactionPGXCancelledContextSecondFunction)
}

func testTransactionPGXCancelledContextFirstFunction(t *testing.T) {
	t.Parallel()
	db := &mocks.Pool{}
	defer db.AssertExpectations(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	total := 4
	tr, err := dbtools.NewTransaction(db, dbtools.RetryCount(total*10))
	require.NoError(t, err)

	tx := &mocks.PGXTx{}
	defer tx.AssertExpectations(t)

	db.On("Begin", mock.Anything).Return(tx, nil).
		Times(total)
	tx.On("Rollback", mock.Anything).Return(nil).
		Times(total)

	calls := 0
	err = tr.PGX(ctx, func(pgx.Tx) error {
		calls++
		// retry package stops it.
		if calls >= total-1 {
			cancel()
		}
		return assert.AnError
	})
	assertInError(t, err, context.Canceled)
	assert.Equal(t, total-1, calls)
}

func testTransactionPGXCancelledContextSecondFunction(t *testing.T) {
	t.Parallel()
	db := &mocks.Pool{}
	defer db.AssertExpectations(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	total := 4
	tr, err := dbtools.NewTransaction(db, dbtools.RetryCount(total*10))
	require.NoError(t, err)

	tx := &mocks.PGXTx{}
	defer tx.AssertExpectations(t)

	db.On("Begin", mock.Anything).Return(tx, nil).
		Times(total)
	tx.On("Rollback", mock.Anything).Return(nil).
		Times(total)

	calls := 0
	err = tr.PGX(ctx, func(pgx.Tx) error {
		calls++
		// our loop catches it.
		if calls >= total {
			cancel()
		}
		return nil
	}, func(pgx.Tx) error {
		return assert.AnError
	})
	assertInError(t, err, context.Canceled)
	assert.Equal(t, total, calls)
}

func testTransactionPGXPanic(t *testing.T) {
	t.Parallel()
	db := &mocks.Pool{}
	defer db.AssertExpectations(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	total := 4
	tr, err := dbtools.NewTransaction(db, dbtools.RetryCount(total))
	require.NoError(t, err)

	tx := &mocks.PGXTx{}
	defer tx.AssertExpectations(t)

	db.On("Begin", mock.Anything).Return(tx, nil).
		Times(total)
	tx.On("Rollback", mock.Anything).Return(nil).
		Times(total)

	calls := 0
	err = tr.PGX(ctx, func(pgx.Tx) error {
		calls++
		panic(assert.AnError.Error())
	})
	assertInError(t, err, assert.AnError)
	assert.Equal(t, total, calls)
}

func testTransactionPGXAnError(t *testing.T) {
	t.Parallel()
	db := &mocks.Pool{}
	defer db.AssertExpectations(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	total := 4
	tr, err := dbtools.NewTransaction(db, dbtools.RetryCount(total))
	require.NoError(t, err)

	tx := &mocks.PGXTx{}
	defer tx.AssertExpectations(t)

	db.On("Begin", mock.Anything).Return(tx, nil).
		Times(total)
	tx.On("Rollback", mock.Anything).Return(nil).
		Times(total)

	calls := 0
	err = tr.PGX(ctx, func(pgx.Tx) error {
		calls++
		return assert.AnError
	})
	assertInError(t, err, assert.AnError)
	assert.Equal(t, total, calls)
}

func testTransactionPGXRollbackError(t *testing.T) {
	t.Parallel()
	db := &mocks.Pool{}
	defer db.AssertExpectations(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	total := 4
	tr, err := dbtools.NewTransaction(db, dbtools.RetryCount(total))
	require.NoError(t, err)

	tx := &mocks.PGXTx{}
	defer tx.AssertExpectations(t)

	db.On("Begin", mock.Anything).Return(tx, nil).
		Times(total)
	tx.On("Rollback", mock.Anything).Return(assert.AnError).
		Times(total)

	calls := 0
	err = tr.PGX(ctx, func(pgx.Tx) error {
		calls++
		panic(randomString(10))
	})
	assertInError(t, err, assert.AnError)
	assert.Equal(t, total, calls)
}

func testTransactionPGXCommitError(t *testing.T) {
	t.Parallel()
	db := &mocks.Pool{}
	defer db.AssertExpectations(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	total := 4
	tr, err := dbtools.NewTransaction(db, dbtools.RetryCount(total))
	require.NoError(t, err)

	tx := &mocks.PGXTx{}
	defer tx.AssertExpectations(t)

	db.On("Begin", mock.Anything).Return(tx, nil).
		Times(total)
	tx.On("Commit", mock.Anything).Return(assert.AnError).
		Times(total)

	calls := 0
	err = tr.PGX(ctx, func(pgx.Tx) error {
		calls++
		return nil
	})
	assertInError(t, err, assert.AnError)
	assert.Equal(t, total, calls)
}

func testTransactionPGXShortStop(t *testing.T) {
	t.Parallel()
	db := &mocks.Pool{}
	defer db.AssertExpectations(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	total := 3
	tr, err := dbtools.NewTransaction(db, dbtools.RetryCount(total*10))
	require.NoError(t, err)

	tx := &mocks.PGXTx{}
	defer tx.AssertExpectations(t)

	db.On("Begin", mock.Anything).Return(tx, nil).
		Times(total)
	tx.On("Rollback", mock.Anything).Return(nil).Times(total)

	calls := 0
	err = tr.PGX(ctx, func(pgx.Tx) error {
		calls++
		if calls >= total {
			return retry.StopError{Err: assert.AnError}
		}
		return errors.New(randomString(10))
	})
	assertInError(t, err, assert.AnError)
	assert.Equal(t, total, calls)
}

func testTransactionPGXRetrySuccess(t *testing.T) {
	t.Parallel()
	db := &mocks.Pool{}
	defer db.AssertExpectations(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	total := 4
	tr, err := dbtools.NewTransaction(db, dbtools.RetryCount(total*10))
	require.NoError(t, err)

	tx := &mocks.PGXTx{}
	defer tx.AssertExpectations(t)

	db.On("Begin", mock.Anything).Return(tx, nil).
		Times(total)
	tx.On("Rollback", mock.Anything).Return(nil).Times(total - 1)
	tx.On("Commit", mock.Anything).Return(nil).Once()

	calls := 0
	err = tr.PGX(ctx, func(pgx.Tx) error {
		calls++
		if calls >= total {
			return nil
		}
		return assert.AnError
	})
	assert.NoError(t, err)
	assert.Equal(t, total, calls)
}

func testTransactionPGXMultipleFunctions(t *testing.T) {
	t.Parallel()
	db := &mocks.Pool{}
	defer db.AssertExpectations(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	total := 4
	tr, err := dbtools.NewTransaction(db, dbtools.RetryCount(total*10))
	require.NoError(t, err)

	tx := &mocks.PGXTx{}
	defer tx.AssertExpectations(t)

	db.On("Begin", mock.Anything).Return(tx, nil)
	tx.On("Rollback", mock.Anything).Return(nil)
	tx.On("Commit", mock.Anything).Return(nil).Once()

	callsFn1 := 0
	callsFn2 := 0
	err = tr.PGX(ctx, func(pgx.Tx) error {
		callsFn1++
		if callsFn1 >= total {
			return nil
		}
		return assert.AnError
	}, func(pgx.Tx) error {
		callsFn2++
		if callsFn2 >= 3 {
			return nil
		}
		return assert.AnError
	})
	assert.NoError(t, err)
	assert.Equal(t, total+2, callsFn1, "expected three turns")
	assert.Equal(t, 3, callsFn2)
}

func testTransactionPGXRealDatabase(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("slow test")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	addr := getDB(t)
	config, err := pgxpool.ParseConfig(addr)
	require.NoError(t, err)
	db, err := pgxpool.ConnectConfig(ctx, config)
	require.NoError(t, err)

	tr, err := dbtools.NewTransaction(db, dbtools.RetryCount(10))
	require.NoError(t, err)

	names := []string{
		randomString(10),
		randomString(20),
		randomString(30),
	}
	gotNames := []string{}
	err = tr.PGX(ctx, func(tx pgx.Tx) error {
		query := `CREATE TABLE pgxtest (name VARCHAR(100) NOT NULL)`
		_, err := tx.Exec(ctx, query)
		return err
	}, func(tx pgx.Tx) error {
		query := `INSERT INTO pgxtest (name) VALUES ($1), ($2), ($3)`
		_, err := tx.Exec(ctx, query, names[0], names[1], names[2])
		return err
	}, func(tx pgx.Tx) error {
		query := `SELECT name FROM pgxtest`
		rows, err := tx.Query(ctx, query)
		if err != nil {
			return err
		}
		for rows.Next() {
			var got string
			err := rows.Scan(&got)
			if err != nil {
				return err
			}
			gotNames = append(gotNames, got)
		}
		return rows.Err()
	})
	require.NoError(t, err)
	assert.ElementsMatch(t, names, gotNames)
}

func testTransactionDB(t *testing.T) {
	t.Run("NilDatabase", testTransactionDBNilDatabase)
	t.Run("BeginError", testTransactionDBBeginError)
	t.Run("CancelledContext", testTransactionDBCancelledContext)
	t.Run("Panic", testTransactionDBPanic)
	t.Run("AnError", testTransactionDBAnError)
	t.Run("RollbackError", testTransactionDBRollbackError)
	t.Run("CommitError", testTransactionDBCommitError)
	t.Run("ShortStop", testTransactionDBShortStop)
	t.Run("RetrySuccess", testTransactionDBRetrySuccess)
	t.Run("MultipleFunctions", testTransactionDBMultipleFunctions)
	t.Run("RealDatabase", testTransactionDBRealDatabase)
}

func testTransactionDBNilDatabase(t *testing.T) {
	t.Parallel()
	db := &mocks.Pool{}
	defer db.AssertExpectations(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tr, err := dbtools.NewTransaction(db)
	require.NoError(t, err)

	err = tr.DB(ctx, func(dbtools.Tx) error {
		t.Error("didn't expect to receive this call")
		return nil
	})
	assertInError(t, err, dbtools.ErrEmptyDatabase)

	tr = &dbtools.Transaction{}
	err = tr.DB(ctx, func(dbtools.Tx) error {
		t.Error("didn't expect to receive this call")
		return nil
	})
	assertInError(t, err, dbtools.ErrEmptyDatabase)
}

func testTransactionDBBeginError(t *testing.T) {
	t.Parallel()
	db := &mocks.DB{}
	defer db.AssertExpectations(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	total := 3
	tr, err := dbtools.NewTransaction(db, dbtools.RetryCount(total))
	require.NoError(t, err)

	db.On("BeginTx", mock.Anything, mock.Anything).
		Return(nil, assert.AnError).Times(total)

	err = tr.DB(ctx, func(dbtools.Tx) error {
		t.Error("didn't expect to receive this call")
		return nil
	})
	assertInError(t, err, assert.AnError)
}

func testTransactionDBCancelledContext(t *testing.T) {
	t.Run("FirstFunction", testTransactionDBCancelledContextFirstFunction)
	t.Run("SecondFunction", testTransactionDBCancelledContextSecondFunction)
}

func testTransactionDBCancelledContextFirstFunction(t *testing.T) {
	t.Parallel()
	db := &mocks.DB{}
	defer db.AssertExpectations(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	total := 4
	tr, err := dbtools.NewTransaction(db, dbtools.RetryCount(total*10))
	require.NoError(t, err)

	tx := &mocks.Tx{}
	defer tx.AssertExpectations(t)

	db.On("BeginTx", mock.Anything, mock.Anything).Return(tx, nil).
		Times(total)
	tx.On("Rollback").Return(nil).
		Times(total)

	calls := 0
	err = tr.DB(ctx, func(dbtools.Tx) error {
		calls++
		// retry package stops it.
		if calls >= total-1 {
			cancel()
		}
		return assert.AnError
	})
	assertInError(t, err, context.Canceled)
	assert.Equal(t, total-1, calls)
}

func testTransactionDBCancelledContextSecondFunction(t *testing.T) {
	t.Parallel()
	db := &mocks.DB{}
	defer db.AssertExpectations(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	total := 4
	tr, err := dbtools.NewTransaction(db, dbtools.RetryCount(total*10))
	require.NoError(t, err)

	tx := &mocks.Tx{}
	defer tx.AssertExpectations(t)

	db.On("BeginTx", mock.Anything, mock.Anything).Return(tx, nil).
		Times(total)
	tx.On("Rollback").Return(nil).
		Times(total)

	calls := 0
	err = tr.DB(ctx, func(dbtools.Tx) error {
		calls++
		// our loop catches it.
		if calls >= total {
			cancel()
		}
		return nil
	}, func(dbtools.Tx) error {
		return assert.AnError
	})
	assertInError(t, err, context.Canceled)
	assert.Equal(t, total, calls)
}

func testTransactionDBPanic(t *testing.T) {
	t.Parallel()
	db := &mocks.DB{}
	defer db.AssertExpectations(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	total := 4
	tr, err := dbtools.NewTransaction(db, dbtools.RetryCount(total))
	require.NoError(t, err)

	tx := &mocks.Tx{}
	defer tx.AssertExpectations(t)

	db.On("BeginTx", mock.Anything, mock.Anything).Return(tx, nil).
		Times(total)
	tx.On("Rollback").Return(nil).
		Times(total)

	calls := 0
	err = tr.DB(ctx, func(dbtools.Tx) error {
		calls++
		panic(assert.AnError.Error())
	})
	assertInError(t, err, assert.AnError)
	assert.Equal(t, total, calls)
}

func testTransactionDBAnError(t *testing.T) {
	t.Parallel()
	db := &mocks.DB{}
	defer db.AssertExpectations(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	total := 4
	tr, err := dbtools.NewTransaction(db, dbtools.RetryCount(total))
	require.NoError(t, err)

	tx := &mocks.Tx{}
	defer tx.AssertExpectations(t)

	db.On("BeginTx", mock.Anything, mock.Anything).Return(tx, nil).
		Times(total)
	tx.On("Rollback").Return(nil).
		Times(total)

	calls := 0
	err = tr.DB(ctx, func(dbtools.Tx) error {
		calls++
		return assert.AnError
	})
	assertInError(t, err, assert.AnError)
	assert.Equal(t, total, calls)
}

func testTransactionDBRollbackError(t *testing.T) {
	t.Parallel()
	db := &mocks.DB{}
	defer db.AssertExpectations(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	total := 4
	tr, err := dbtools.NewTransaction(db, dbtools.RetryCount(total))
	require.NoError(t, err)

	tx := &mocks.Tx{}
	defer tx.AssertExpectations(t)

	db.On("BeginTx", mock.Anything, mock.Anything).Return(tx, nil).
		Times(total)
	tx.On("Rollback").Return(assert.AnError).
		Times(total)

	calls := 0
	err = tr.DB(ctx, func(dbtools.Tx) error {
		calls++
		panic(randomString(10))
	})
	assertInError(t, err, assert.AnError)
	assert.Equal(t, total, calls)
}

func testTransactionDBCommitError(t *testing.T) {
	t.Parallel()
	db := &mocks.DB{}
	defer db.AssertExpectations(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	total := 4
	tr, err := dbtools.NewTransaction(db, dbtools.RetryCount(total))
	require.NoError(t, err)

	tx := &mocks.Tx{}
	defer tx.AssertExpectations(t)

	db.On("BeginTx", mock.Anything, mock.Anything).Return(tx, nil).
		Times(total)
	tx.On("Commit").Return(assert.AnError).
		Times(total)

	calls := 0
	err = tr.DB(ctx, func(dbtools.Tx) error {
		calls++
		return nil
	})
	assertInError(t, err, assert.AnError)
	assert.Equal(t, total, calls)
}

func testTransactionDBShortStop(t *testing.T) {
	t.Parallel()
	db := &mocks.DB{}
	defer db.AssertExpectations(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	total := 3
	tr, err := dbtools.NewTransaction(db, dbtools.RetryCount(total*10))
	require.NoError(t, err)

	tx := &mocks.Tx{}
	defer tx.AssertExpectations(t)

	db.On("BeginTx", mock.Anything, mock.Anything).Return(tx, nil).
		Times(total)
	tx.On("Rollback").Return(nil).Times(total)

	calls := 0
	err = tr.DB(ctx, func(dbtools.Tx) error {
		calls++
		if calls >= total {
			return retry.StopError{Err: assert.AnError}
		}
		return errors.New(randomString(10))
	})
	assertInError(t, err, assert.AnError)
	assert.Equal(t, total, calls)
}

func testTransactionDBRetrySuccess(t *testing.T) {
	t.Parallel()
	db := &mocks.DB{}
	defer db.AssertExpectations(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	total := 4
	tr, err := dbtools.NewTransaction(db, dbtools.RetryCount(total*10))
	require.NoError(t, err)

	tx := &mocks.Tx{}
	defer tx.AssertExpectations(t)

	db.On("BeginTx", mock.Anything, mock.Anything).Return(tx, nil).
		Times(total)
	tx.On("Rollback").Return(nil).Times(total - 1)
	tx.On("Commit").Return(nil).Once()

	calls := 0
	err = tr.DB(ctx, func(dbtools.Tx) error {
		calls++
		if calls >= total {
			return nil
		}
		return assert.AnError
	})
	assert.NoError(t, err)
	assert.Equal(t, total, calls)
}

func testTransactionDBMultipleFunctions(t *testing.T) {
	t.Parallel()
	db := &mocks.DB{}
	defer db.AssertExpectations(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	total := 4
	tr, err := dbtools.NewTransaction(db, dbtools.RetryCount(total*10))
	require.NoError(t, err)

	tx := &mocks.Tx{}
	defer tx.AssertExpectations(t)

	db.On("BeginTx", mock.Anything, mock.Anything).Return(tx, nil)
	tx.On("Rollback").Return(nil)
	tx.On("Commit").Return(nil).Once()

	callsFn1 := 0
	callsFn2 := 0
	err = tr.DB(ctx, func(dbtools.Tx) error {
		callsFn1++
		if callsFn1 >= total {
			return nil
		}
		return assert.AnError
	}, func(dbtools.Tx) error {
		callsFn2++
		if callsFn2 >= 3 {
			return nil
		}
		return assert.AnError
	})
	assert.NoError(t, err)
	assert.Equal(t, total+2, callsFn1, "expected three turns")
	assert.Equal(t, 3, callsFn2)
}

func testTransactionDBRealDatabase(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("slow test")
	}
	addr := getDB(t)
	config, err := pgx.ParseConfig(addr)
	require.NoError(t, err)
	db := stdlib.OpenDB(*config)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tr, err := dbtools.NewTransaction(db, dbtools.RetryCount(10))
	require.NoError(t, err)

	names := []string{
		randomString(10),
		randomString(20),
		randomString(30),
	}
	gotNames := []string{}
	err = tr.DB(ctx, func(tx dbtools.Tx) error {
		query := `CREATE TABLE dbtest (name VARCHAR(100) NOT NULL)`
		_, err := tx.ExecContext(ctx, query)
		return err
	}, func(tx dbtools.Tx) error {
		query := `INSERT INTO dbtest (name) VALUES ($1), ($2), ($3)`
		_, err := tx.ExecContext(ctx, query, names[0], names[1], names[2])
		return err
	}, func(tx dbtools.Tx) error {
		query := `SELECT name FROM dbtest`
		rows, err := tx.QueryContext(ctx, query)
		if err != nil {
			return err
		}
		for rows.Next() {
			var got string
			err := rows.Scan(&got)
			if err != nil {
				return err
			}
			gotNames = append(gotNames, got)
		}
		return rows.Err()
	})
	require.NoError(t, err)
	assert.ElementsMatch(t, names, gotNames)
}

func TestWithTransaction(t *testing.T) {
	t.Run("BeginCommit", testWithTransactionBeginCommit)
	t.Run("RollbackFirst", testWithTransactionRollbackFirst)
	t.Run("RollbackSecond", testWithTransactionRollbackSecond)
	t.Run("Commit", testWithTransactionCommit)
	t.Run("FunctionPanic", testWithTransactionFunctionPanic)
}

func testWithTransactionBeginCommit(t *testing.T) {
	t.Parallel()
	db, dbMock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	defer func() {
		if err := dbMock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
	}()
	dbMock.ExpectBegin().
		WillReturnError(assert.AnError)
	err = dbtools.WithTransaction(db)
	assert.Equal(t, assert.AnError, errors.Cause(err))

	dbMock.ExpectBegin()
	dbMock.ExpectCommit().
		WillReturnError(assert.AnError)
	err = dbtools.WithTransaction(db)
	assert.Equal(t, assert.AnError, errors.Cause(err))

	dbMock.ExpectBegin()
	dbMock.ExpectCommit()
	err = dbtools.WithTransaction(db)
	assert.NoError(t, err)
}

func testWithTransactionRollbackFirst(t *testing.T) {
	t.Parallel()
	db, dbMock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	defer func() {
		if err := dbMock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
	}()
	dbMock.ExpectBegin()
	dbMock.ExpectRollback()
	err = dbtools.WithTransaction(db, func(*sql.Tx) error {
		return assert.AnError
	}, func(*sql.Tx) error {
		t.Error("didn't expect to be called")
		return nil
	})
	mErr, ok := err.(*multierror.Error)
	require.True(t, ok, "not a multierror")
	var wantErr error
	for _, e := range mErr.Errors {
		if errors.Cause(e) == assert.AnError {
			wantErr = e
		}
	}
	assert.Equal(t, assert.AnError, errors.Cause(wantErr))
}

func testWithTransactionRollbackSecond(t *testing.T) {
	t.Parallel()
	db, dbMock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	defer func() {
		if err := dbMock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
	}()
	dbMock.ExpectBegin()
	dbMock.ExpectRollback()
	err = dbtools.WithTransaction(db, func(*sql.Tx) error {
		return nil
	}, func(*sql.Tx) error {
		return assert.AnError
	})
	mErr, ok := err.(*multierror.Error)
	require.True(t, ok, "not a multierror")
	var wantErr error
	for _, e := range mErr.Errors {
		if errors.Cause(e) == assert.AnError {
			wantErr = e
		}
	}
	assert.Equal(t, assert.AnError, errors.Cause(wantErr))
}

func testWithTransactionCommit(t *testing.T) {
	t.Parallel()
	db, dbMock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	defer func() {
		if err := dbMock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
	}()
	dbMock.ExpectBegin()
	dbMock.ExpectCommit()
	err = dbtools.WithTransaction(db, func(*sql.Tx) error {
		return nil
	})
	assert.NoError(t, err)

	dbMock.ExpectBegin()
	dbMock.ExpectCommit().
		WillReturnError(assert.AnError)
	err = dbtools.WithTransaction(db, func(*sql.Tx) error {
		return nil
	})
	assert.Equal(t, assert.AnError, errors.Cause(err))
}

func testWithTransactionFunctionPanic(t *testing.T) {
	t.Parallel()
	db, dbMock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	defer func() {
		if err := dbMock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
	}()
	dbMock.ExpectBegin()
	dbMock.ExpectRollback()
	require.NotPanics(t, func() {
		err := dbtools.WithTransaction(db, func(*sql.Tx) error {
			panic("for some reason")
		}, func(*sql.Tx) error {
			t.Error("didn't expect to be called")
			return nil
		})
		assert.Error(t, err)
	})
}

func ExampleWithTransaction() {
	// For this example we are using sqlmock, but you can use an actual
	// connection with this function.
	db, dbMock, err := sqlmock.New()
	if err != nil {
		panic(err)
	}
	defer db.Close()
	defer func() {
		if err := dbMock.ExpectationsWereMet(); err != nil {
			fmt.Printf("there were unfulfilled expectations: %s\n", err)
		}
	}()
	dbMock.ExpectBegin()
	dbMock.ExpectCommit()
	err = dbtools.WithTransaction(db, func(*sql.Tx) error {
		fmt.Println("Running first query.")
		return nil
	}, func(*sql.Tx) error {
		fmt.Println("Running second query.")
		return nil
	})
	fmt.Println("Transaction has an error:", err != nil)

	// Output:
	// Running first query.
	// Running second query.
	// Transaction has an error: false
}

func ExampleWithTransaction_two() {
	// For this example we are using sqlmock, but you can use an actual
	// connection with this function.
	db, dbMock, err := sqlmock.New()
	if err != nil {
		panic(err)
	}
	defer db.Close()
	defer func() {
		if err := dbMock.ExpectationsWereMet(); err != nil {
			fmt.Printf("there were unfulfilled expectations: %s\n", err)
		}
	}()
	dbMock.ExpectBegin()
	dbMock.ExpectRollback()
	err = dbtools.WithTransaction(db, func(*sql.Tx) error {
		fmt.Println("Running first query.")
		return nil
	}, func(*sql.Tx) error {
		fmt.Println("Running second query.")
		return errors.New("something happened")
	}, func(*sql.Tx) error {
		fmt.Println("Running third query.")
		return nil
	})
	fmt.Println("Transaction has an error:", err != nil)

	// Output:
	// Running first query.
	// Running second query.
	// Transaction has an error: true
}

func TestRetry(t *testing.T) {
	t.Run("Delay", testRetryDelay)
	t.Run("Retries", testRetryRetries)
	t.Run("FunctionPanic", testRetryFunctionPanic)
}

func testRetryDelay(t *testing.T) {
	t.Parallel()
	// In this setup, the delays would be 100, 200, 300, 400. So in 1 second
	// there would be 4 calls.
	count := 0
	delay := time.Millisecond * 100
	start := time.Now()
	err := dbtools.Retry(4, delay, func(int) error {
		count++
		return assert.AnError
	})
	latency := time.Since(start)
	assert.Equal(t, 4, count)
	assert.Equal(t, assert.AnError, errors.Cause(err))
	if latency.Nanoseconds()-time.Second.Nanoseconds() > delay.Nanoseconds() {
		t.Errorf("didn't finish in time: %d", latency.Nanoseconds())
	}
}

func testRetryRetries(t *testing.T) {
	t.Parallel()
	retries := 100
	count := 0
	err := dbtools.Retry(retries, time.Nanosecond, func(int) error {
		count++
		if count >= 20 {
			return nil
		}
		return assert.AnError
	})
	assert.NoError(t, err)
	assert.Equal(t, 20, count)
}

func testRetryFunctionPanic(t *testing.T) {
	t.Parallel()
	retries := 100
	count := 0
	assert.NotPanics(t, func() {
		err := dbtools.Retry(retries, time.Nanosecond, func(int) error {
			count++
			panic("for some reason")
		})
		assert.Error(t, err)
	})
	assert.Equal(t, retries, count)
}

func ExampleRetry() {
	err := dbtools.Retry(100, time.Nanosecond, func(i int) error {
		fmt.Printf("Running iteration %d.\n", i+1)
		if i < 1 {
			return errors.New("ignored error")
		}
		return nil
	})
	fmt.Println("Error:", err)

	// Output:
	// Running iteration 1.
	// Running iteration 2.
	// Error: <nil>
}

func ExampleRetry_two() {
	err := dbtools.Retry(2, time.Nanosecond, func(i int) error {
		fmt.Printf("Running iteration %d.\n", i+1)
		return errors.New("some error")
	})
	fmt.Println("Error:", err)

	// Output:
	// Running iteration 1.
	// Running iteration 2.
	// Error: some error
}

func TestRetryTransaction(t *testing.T) {
	t.Run("Retry", testRetryTransactionRetry)
	t.Run("ContextCancelled", testRetryTransactionContextCancelled)
}

func testRetryTransactionRetry(t *testing.T) {
	t.Run("ZeroTimes", testRetryTransactionRetryZeroTimes)
	t.Run("Once", testRetryTransactionRetryOnce)
	t.Run("MoreTime", testRetryTransactionRetryMoreTime)
	t.Run("Error", testRetryTransactionRetryError)
}

func testRetryTransactionRetryZeroTimes(t *testing.T) {
	t.Parallel()
	db, dbMock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	defer func() {
		if err := dbMock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
	}()

	ctx := context.Background()
	err = dbtools.RetryTransaction(ctx, db, 0, time.Nanosecond, func(*sql.Tx) error {
		t.Error("didn't expect the function to be called")
		return nil
	})
	assert.NoError(t, err)
}

func testRetryTransactionRetryOnce(t *testing.T) {
	t.Parallel()
	db, dbMock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	defer func() {
		if err := dbMock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
	}()

	dbMock.ExpectBegin()
	dbMock.ExpectCommit()

	calls := 0
	ctx := context.Background()
	err = dbtools.RetryTransaction(ctx, db, 100, time.Nanosecond, func(*sql.Tx) error {
		calls++
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, calls, 1, "function calls")
}

func testRetryTransactionRetryMoreTime(t *testing.T) {
	t.Parallel()
	db, dbMock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	defer func() {
		if err := dbMock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
	}()

	dbMock.ExpectBegin()
	dbMock.ExpectRollback()
	dbMock.ExpectBegin()
	dbMock.ExpectCommit()

	calls := 0
	ctx := context.Background()
	err = dbtools.RetryTransaction(ctx, db, 100, time.Nanosecond, func(*sql.Tx) error {
		calls++
		if calls < 2 {
			return assert.AnError
		}
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, calls, 2, "function calls")
}

func ExampleRetryTransaction() {
	// For this example we are using sqlmock, but you can use an actual
	// connection with this function.
	db, dbMock, err := sqlmock.New()
	if err != nil {
		panic(err)
	}
	defer db.Close()
	defer func() {
		if err := dbMock.ExpectationsWereMet(); err != nil {
			fmt.Printf("there were unfulfilled expectations: %s\n", err)
		}
	}()

	dbMock.ExpectBegin()
	dbMock.ExpectRollback()
	dbMock.ExpectBegin()
	dbMock.ExpectRollback()
	dbMock.ExpectBegin()
	dbMock.ExpectCommit()

	calls := 0
	ctx := context.Background()
	err = dbtools.RetryTransaction(ctx, db, 100, time.Millisecond*100, func(*sql.Tx) error {
		calls++
		fmt.Printf("Running iteration %d.\n", calls)
		if calls < 3 {
			return errors.New("some error")
		}
		return nil
	})
	fmt.Println("Error:", err)

	// Output:
	// Running iteration 1.
	// Running iteration 2.
	// Running iteration 3.
	// Error: <nil>
}

func testRetryTransactionRetryError(t *testing.T) {
	t.Parallel()
	db, dbMock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	defer func() {
		if err := dbMock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
	}()

	dbMock.ExpectBegin()
	dbMock.ExpectRollback()
	dbMock.ExpectBegin()
	dbMock.ExpectRollback()
	dbMock.ExpectBegin()
	dbMock.ExpectRollback()

	ctx := context.Background()
	err = dbtools.RetryTransaction(ctx, db, 3, time.Nanosecond, func(*sql.Tx) error {
		return assert.AnError
	})
	mErr, ok := err.(*multierror.Error)
	require.True(t, ok, "not a multierror")
	var wantErr error
	for _, e := range mErr.Errors {
		if errors.Cause(e) == assert.AnError {
			wantErr = e
		}
	}
	assert.Equal(t, assert.AnError, wantErr, "want context cancelled instead")
}

func testRetryTransactionContextCancelled(t *testing.T) {
	t.Run("AtBeginning", testRetryTransactionContextCancelledAtBeginning)
	t.Run("FirstRetry", testRetryTransactionContextCancelledFirstRetry)
	t.Run("SecondRetry", testRetryTransactionContextCancelledSecondRetry)
}

func testRetryTransactionContextCancelledAtBeginning(t *testing.T) {
	t.Parallel()
	db, dbMock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	defer func() {
		if err := dbMock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = dbtools.RetryTransaction(ctx, db, 100, time.Nanosecond, func(*sql.Tx) error {
		t.Error("didn't expect to get called")
		return nil
	})
	assert.Equal(t, ctx.Err(), err)
}

func testRetryTransactionContextCancelledFirstRetry(t *testing.T) {
	t.Parallel()
	db, dbMock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	defer func() {
		if err := dbMock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
	}()
	dbMock.ExpectBegin()
	dbMock.ExpectRollback()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	called := false
	err = dbtools.RetryTransaction(ctx, db, 100, time.Nanosecond, func(*sql.Tx) error {
		assert.False(t, called, "didn't expect to get called")
		cancel()
		called = true
		return assert.AnError
	})
	assert.Equal(t, ctx.Err(), errors.Cause(err))
}

func testRetryTransactionContextCancelledSecondRetry(t *testing.T) {
	t.Parallel()
	db, dbMock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	defer func() {
		if err := dbMock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
	}()
	dbMock.ExpectBegin()
	dbMock.ExpectRollback()
	dbMock.ExpectBegin()
	dbMock.ExpectRollback()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	err = dbtools.RetryTransaction(ctx, db, 100, time.Nanosecond, func(*sql.Tx) error {
		calls++
		if calls < 2 {
			return assert.AnError
		}
		cancel()
		return assert.AnError
	})
	assert.Equal(t, ctx.Err(), errors.Cause(err))
}
