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

func TestNewTransaction(t *testing.T) {
	// t.Parallel()
	// db, _, err := sqlmock.New()
	// require.NoError(t, err)
	// t.Cleanup(func() {
	// 	db.Close()
	// })
	// tcs := map[string]struct {
	// 	db      interface{}
	// 	conf    []dbtools.ConfigFunc
	// 	wantErr error
	// }{
	// 	"nil db":        {nil, nil, dbtools.ErrEmptyDatabase},
	// 	"nil pgxpool":   {nil, nil, dbtools.ErrEmptyDatabase},
	// 	"wrong db type": {"db", nil, dbtools.ErrEmptyDatabase},
	// 	"pool":          {&mocks.Pool{}, nil, nil},
	// 	"low attempts":  {db, []dbtools.ConfigFunc{dbtools.RetryCount(-1)}, nil},
	// 	"delay method":  {db, []dbtools.ConfigFunc{dbtools.DelayMethod(retry.IncrementalDelay)}, nil},
	// 	"retrier":       {db, []dbtools.ConfigFunc{dbtools.Retry(retry.Retry{})}, nil},
	// }
	// for name, tc := range tcs {
	// 	tc := tc
	// 	t.Run(name, func(t *testing.T) {
	// 		t.Parallel()
	// 		_, err := dbtools.NewTransaction(tc.db, tc.conf...)
	// 		if tc.wantErr == nil {
	// 			assert.NoError(t, err)
	// 			return
	// 		}
	// 		assertInError(t, err, tc.wantErr)
	// 	})
	// }
}

func TestTransaction(t *testing.T) {
	t.Parallel()
	t.Run("PGX", testTransactionPGX)
}

func testTransactionPGX(t *testing.T) {
	t.Parallel()
	t.Run("NilDatabase", testTransactionPGXNilDatabase)
	t.Run("BeginError", testTransactionPGXBeginError)
	t.Run("CancelledContext", testTransactionPGXCancelledContext)
	t.Run("Panic", testTransactionPGXPanic)
	t.Run("AnError", testTransactionPGXAnError)
	t.Run("ErrorIs", testTransactionPGXErrorIs)
	t.Run("RollbackError", testTransactionPGXRollbackError)
	t.Run("CommitError", testTransactionPGXCommitError)
	t.Run("ShortStop", testTransactionPGXShortStop)
	t.Run("RetrySuccess", testTransactionPGXRetrySuccess)
	t.Run("MultipleFunctions", testTransactionPGXMultipleFunctions)
	t.Run("RealDatabase", testTransactionPGXRealDatabase)
	t.Run("ContextCancelled", testTransactionPGXContextCancelled)
}

func testTransactionPGXNilDatabase(t *testing.T) {
	t.Parallel()
	db := &mocks.Pool{}
	defer db.AssertExpectations(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tr := &dbtools.Transaction{}
	err := tr.PGX(ctx, func(pgx.Tx) error {
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

func testTransactionPGXErrorIs(t *testing.T) {
	t.Parallel()
	db := &mocks.Pool{}
	defer db.AssertExpectations(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tr, err := dbtools.NewTransaction(db, dbtools.RetryCount(1))
	require.NoError(t, err)

	tx := &mocks.PGXTx{}
	defer tx.AssertExpectations(t)

	db.On("Begin", mock.Anything).Return(tx, nil)
	tx.On("Rollback", mock.Anything).Return(nil).Maybe()

	err = tr.PGX(ctx, func(pgx.Tx) error {
		return &retry.StopError{Err: assert.AnError}
	})
	assert.True(t, errors.Is(err, assert.AnError))
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
	t.Run("WithValue", testTransactionPGXShortStopWithValue)
	t.Run("WithPointer", testTransactionPGXShortStopWithPointer)
}

func testTransactionPGXShortStopWithValue(t *testing.T) {
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

func testTransactionPGXShortStopWithPointer(t *testing.T) {
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
			return &retry.StopError{Err: assert.AnError}
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

// nolint:wrapcheck // no need to check these.
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

func testTransactionPGXContextCancelled(t *testing.T) {
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

	calls := 0
	// we are not using the same context to make sure the query causes the
	// error.
	err = tr.PGX(ctx, func(tx pgx.Tx) error {
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
