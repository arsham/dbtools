package dbtools_test

import (
	"context"
	"testing"

	"github.com/arsham/dbtools/v2"
	"github.com/arsham/dbtools/v2/mocks"
	"github.com/arsham/retry"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestNewPGX(t *testing.T) {
	t.Parallel()
	db := mocks.NewPool(t)
	tcs := map[string]struct {
		db      dbtools.Pool
		conf    []dbtools.ConfigFunc
		wantErr error
	}{
		"nil db":       {nil, nil, dbtools.ErrEmptyDatabase},
		"low attempts": {db, []dbtools.ConfigFunc{dbtools.RetryCount(-1)}, nil},
		"delay method": {db, []dbtools.ConfigFunc{dbtools.DelayMethod(retry.IncrementalDelay)}, nil},
		"retrier":      {db, []dbtools.ConfigFunc{dbtools.Retry(retry.Retry{})}, nil},
		"all ok":       {db, nil, nil},
	}
	for name, tc := range tcs {
		tc := tc
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := dbtools.NewPGX(tc.db, tc.conf...)
			if tc.wantErr == nil {
				assert.NoError(t, err)
				return
			}
			assertInError(t, err, tc.wantErr)
		})
	}
}

func TestPGX(t *testing.T) {
	t.Parallel()
	t.Run("NilDatabase", testPGXTransactionNilDatabase)
	t.Run("BeginError", testPGXTransactionBeginError)
	t.Run("CancelledContext", testPGXTransactionCancelledContext)
	t.Run("Panic", testPGXTransactionPanic)
	t.Run("AnError", testPGXTransactionAnError)
	t.Run("ErrorIs", testPGXTransactionErrorIs)
	t.Run("RollbackError", testPGXTransactionRollbackError)
	t.Run("CommitError", testPGXTransactionCommitError)
	t.Run("ShortStop", testPGXTransactionShortStop)
	t.Run("RetrySuccess", testPGXTransactionRetrySuccess)
	t.Run("MultipleFunctions", testPGXTransactionMultipleFunctions)
	t.Run("RealDatabase", testPGXTransactionRealDatabase)
	t.Run("ContextCancelled", testPGXTransactionContextCancelled)
}

func testPGXTransactionNilDatabase(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tr := &dbtools.PGX{}
	err := tr.Transaction(ctx, func(pgx.Tx) error {
		t.Error("didn't expect to receive this call")
		return nil
	})
	assertInError(t, err, dbtools.ErrEmptyDatabase)
}

func testPGXTransactionBeginError(t *testing.T) {
	t.Parallel()
	db := mocks.NewPool(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	total := 3
	tr, err := dbtools.NewPGX(db, dbtools.RetryCount(total))
	require.NoError(t, err)

	db.On("Begin", mock.Anything).
		Return(nil, assert.AnError).Times(total)

	err = tr.Transaction(ctx, func(pgx.Tx) error {
		t.Error("didn't expect to receive this call")
		return nil
	})
	assertInError(t, err, assert.AnError)
}

func testPGXTransactionCancelledContext(t *testing.T) {
	t.Run("FirstFunction", testPGXTransactionCancelledContextFirstFunction)
	t.Run("SecondFunction", testPGXTransactionCancelledContextSecondFunction)
}

func testPGXTransactionCancelledContextFirstFunction(t *testing.T) {
	t.Parallel()
	db := mocks.NewPool(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	total := 4
	tr, err := dbtools.NewPGX(db, dbtools.RetryCount(total*10))
	require.NoError(t, err)

	tx := mocks.NewPGXTx(t)
	db.On("Begin", mock.Anything).Return(tx, nil).
		Times(total)
	tx.On("Rollback", mock.Anything).Return(nil).
		Times(total)

	calls := 0
	err = tr.Transaction(ctx, func(pgx.Tx) error {
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

func testPGXTransactionCancelledContextSecondFunction(t *testing.T) {
	t.Parallel()
	db := mocks.NewPool(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	total := 4
	tr, err := dbtools.NewPGX(db, dbtools.RetryCount(total*10))
	require.NoError(t, err)

	tx := mocks.NewPGXTx(t)
	db.On("Begin", mock.Anything).Return(tx, nil).
		Times(total)
	tx.On("Rollback", mock.Anything).Return(nil).
		Times(total)

	calls := 0
	err = tr.Transaction(ctx, func(pgx.Tx) error {
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

func testPGXTransactionPanic(t *testing.T) {
	t.Parallel()
	db := mocks.NewPool(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	total := 4
	tr, err := dbtools.NewPGX(db, dbtools.RetryCount(total))
	require.NoError(t, err)

	tx := mocks.NewPGXTx(t)
	db.On("Begin", mock.Anything).Return(tx, nil).
		Times(total)
	tx.On("Rollback", mock.Anything).Return(nil).
		Times(total)

	calls := 0
	err = tr.Transaction(ctx, func(pgx.Tx) error {
		calls++
		panic(assert.AnError.Error())
	})
	assertInError(t, err, assert.AnError)
	assert.Equal(t, total, calls)
}

func testPGXTransactionAnError(t *testing.T) {
	t.Run("NoRollbackError", testPGXTransactionAnErrorNoRollbackError)
	t.Run("WithRollbackError", testPGXTransactionAnErrorWithRollbackError)
}

func testPGXTransactionAnErrorNoRollbackError(t *testing.T) {
	t.Parallel()
	db := mocks.NewPool(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	total := 4
	tr, err := dbtools.NewPGX(db, dbtools.RetryCount(total))
	require.NoError(t, err)

	tx := mocks.NewPGXTx(t)
	db.On("Begin", mock.Anything).Return(tx, nil).
		Times(total)
	tx.On("Rollback", mock.Anything).Return(nil).
		Times(total)

	calls := 0
	err = tr.Transaction(ctx, func(pgx.Tx) error {
		calls++
		return assert.AnError
	})
	assertInError(t, err, assert.AnError)
	assert.Equal(t, total, calls)
}

func testPGXTransactionAnErrorWithRollbackError(t *testing.T) {
	t.Parallel()
	db := mocks.NewPool(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	total := 4
	tr, err := dbtools.NewPGX(db, dbtools.RetryCount(total))
	require.NoError(t, err)

	trError := errors.New("in transaction")
	rollbackError := errors.New("from rollback")

	tx := mocks.NewPGXTx(t)
	db.On("Begin", mock.Anything).Return(tx, nil)
	tx.On("Rollback", mock.Anything).Return(rollbackError)

	err = tr.Transaction(ctx, func(pgx.Tx) error {
		return trError
	})
	assertInError(t, err, trError)
	assertInError(t, err, rollbackError)
}

func testPGXTransactionErrorIs(t *testing.T) {
	t.Parallel()
	db := mocks.NewPool(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tr, err := dbtools.NewPGX(db, dbtools.RetryCount(1))
	require.NoError(t, err)

	tx := mocks.NewPGXTx(t)
	db.On("Begin", mock.Anything).Return(tx, nil)
	tx.On("Rollback", mock.Anything).Return(nil).Maybe()

	err = tr.Transaction(ctx, func(pgx.Tx) error {
		return &retry.StopError{Err: assert.AnError}
	})
	assert.True(t, errors.Is(err, assert.AnError))
}

func testPGXTransactionRollbackError(t *testing.T) {
	t.Parallel()
	db := mocks.NewPool(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	total := 4
	tr, err := dbtools.NewPGX(db, dbtools.RetryCount(total))
	require.NoError(t, err)

	tx := mocks.NewPGXTx(t)
	db.On("Begin", mock.Anything).Return(tx, nil).
		Times(total)
	tx.On("Rollback", mock.Anything).Return(assert.AnError).
		Times(total)

	calls := 0
	err = tr.Transaction(ctx, func(pgx.Tx) error {
		calls++
		panic(randomString(10))
	})
	assertInError(t, err, assert.AnError)
	assert.Equal(t, total, calls)
}

func testPGXTransactionCommitError(t *testing.T) {
	t.Parallel()
	db := mocks.NewPool(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	total := 4
	tr, err := dbtools.NewPGX(db, dbtools.RetryCount(total))
	require.NoError(t, err)

	tx := mocks.NewPGXTx(t)
	db.On("Begin", mock.Anything).Return(tx, nil).
		Times(total)
	tx.On("Commit", mock.Anything).Return(assert.AnError).
		Times(total)

	calls := 0
	err = tr.Transaction(ctx, func(pgx.Tx) error {
		calls++
		return nil
	})
	assertInError(t, err, assert.AnError)
	assert.Equal(t, total, calls)
}

func testPGXTransactionShortStop(t *testing.T) {
	t.Run("WithValue", testPGXTransactionShortStopWithValue)
	t.Run("WithPointer", testPGXTransactionShortStopWithPointer)
}

func testPGXTransactionShortStopWithValue(t *testing.T) {
	t.Parallel()
	db := mocks.NewPool(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	total := 3
	tr, err := dbtools.NewPGX(db, dbtools.RetryCount(total*10))
	require.NoError(t, err)

	tx := mocks.NewPGXTx(t)
	db.On("Begin", mock.Anything).Return(tx, nil).
		Times(total)
	tx.On("Rollback", mock.Anything).Return(nil).Times(total)

	calls := 0
	err = tr.Transaction(ctx, func(pgx.Tx) error {
		calls++
		if calls >= total {
			return retry.StopError{Err: assert.AnError}
		}
		return errors.New(randomString(10))
	})
	assertInError(t, err, assert.AnError)
	assert.Equal(t, total, calls)
}

func testPGXTransactionShortStopWithPointer(t *testing.T) {
	t.Parallel()
	db := mocks.NewPool(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	total := 3
	tr, err := dbtools.NewPGX(db, dbtools.RetryCount(total*10))
	require.NoError(t, err)

	tx := mocks.NewPGXTx(t)
	db.On("Begin", mock.Anything).Return(tx, nil).
		Times(total)
	tx.On("Rollback", mock.Anything).Return(nil).Times(total)

	calls := 0
	err = tr.Transaction(ctx, func(pgx.Tx) error {
		calls++
		if calls >= total {
			return &retry.StopError{Err: assert.AnError}
		}
		return errors.New(randomString(10))
	})
	assertInError(t, err, assert.AnError)
	assert.Equal(t, total, calls)
}

func testPGXTransactionRetrySuccess(t *testing.T) {
	t.Parallel()
	db := mocks.NewPool(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	total := 4
	tr, err := dbtools.NewPGX(db, dbtools.RetryCount(total*10))
	require.NoError(t, err)

	tx := mocks.NewPGXTx(t)
	db.On("Begin", mock.Anything).Return(tx, nil).
		Times(total)
	tx.On("Rollback", mock.Anything).Return(nil).Times(total - 1)
	tx.On("Commit", mock.Anything).Return(nil).Once()

	calls := 0
	err = tr.Transaction(ctx, func(pgx.Tx) error {
		calls++
		if calls >= total {
			return nil
		}
		return assert.AnError
	})
	assert.NoError(t, err)
	assert.Equal(t, total, calls)
}

func testPGXTransactionMultipleFunctions(t *testing.T) {
	t.Parallel()
	db := mocks.NewPool(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	total := 4
	tr, err := dbtools.NewPGX(db, dbtools.RetryCount(total*10))
	require.NoError(t, err)

	tx := mocks.NewPGXTx(t)
	db.On("Begin", mock.Anything).Return(tx, nil)
	tx.On("Rollback", mock.Anything).Return(nil)
	tx.On("Commit", mock.Anything).Return(nil).Once()

	callsFn1 := 0
	callsFn2 := 0
	err = tr.Transaction(ctx, func(pgx.Tx) error {
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

func testPGXTransactionRealDatabase(t *testing.T) {
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

	tr, err := dbtools.NewPGX(db, dbtools.RetryCount(10))
	require.NoError(t, err)

	names := []string{
		randomString(10),
		randomString(20),
		randomString(30),
	}
	gotNames := []string{}
	err = tr.Transaction(ctx, func(tx pgx.Tx) error {
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

func testPGXTransactionContextCancelled(t *testing.T) {
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

	tr, err := dbtools.NewPGX(db, dbtools.RetryCount(10))
	require.NoError(t, err)

	calls := 0
	// we are not using the same context to make sure the query causes the
	// error.
	err = tr.Transaction(ctx, func(tx pgx.Tx) error {
		calls++
		query := `CREATE TABLE dbtest (name VARCHAR(100))`
		_, err := tx.Exec(ctx, query)
		return err
	}, func(tx pgx.Tx) error {
		cancel()
		query := `INSERT INTO dbtest (name) VALUES ('a')`
		_, err := tx.Exec(ctx, query)
		return err
	}, func(pgx.Tx) error {
		t.Error("didn't expect to get this")
		return nil
	})
	assertInError(t, err, context.Canceled)
	assert.Equal(t, 1, calls)
}
