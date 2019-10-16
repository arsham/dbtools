package dbtools_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/arsham/dbtools"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWithTransaction(t *testing.T) {
	t.Run("BeginCommit", testWithTransactionBeginCommit)
	t.Run("RollbackFirst", testWithTransactionRollbackFirst)
	t.Run("RollbackSecond", testWithTransactionRollbackSecond)
	t.Run("Commit", testWithTransactionCommit)
	t.Run("FunctionPanic", testWithTransactionFunctionPanic)
}

func testWithTransactionBeginCommit(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	defer func() {
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
	}()
	mock.ExpectBegin().
		WillReturnError(assert.AnError)
	err = dbtools.WithTransaction(db)
	assert.Equal(t, assert.AnError, errors.Cause(err))

	mock.ExpectBegin()
	mock.ExpectCommit().
		WillReturnError(assert.AnError)
	err = dbtools.WithTransaction(db)
	assert.Equal(t, assert.AnError, errors.Cause(err))

	mock.ExpectBegin()
	mock.ExpectCommit()
	err = dbtools.WithTransaction(db)
	assert.NoError(t, err)
}

func testWithTransactionRollbackFirst(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	defer func() {
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
	}()
	mock.ExpectBegin()
	mock.ExpectRollback()
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
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	defer func() {
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
	}()
	mock.ExpectBegin()
	mock.ExpectRollback()
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
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	defer func() {
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
	}()
	mock.ExpectBegin()
	mock.ExpectCommit()
	err = dbtools.WithTransaction(db, func(*sql.Tx) error {
		return nil
	})
	assert.NoError(t, err)

	mock.ExpectBegin()
	mock.ExpectCommit().
		WillReturnError(assert.AnError)
	err = dbtools.WithTransaction(db, func(*sql.Tx) error {
		return nil
	})
	assert.Equal(t, assert.AnError, errors.Cause(err))
}

func testWithTransactionFunctionPanic(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	defer func() {
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
	}()
	mock.ExpectBegin()
	mock.ExpectRollback()
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
	db, mock, err := sqlmock.New()
	if err != nil {
		panic(err)
	}
	defer db.Close()
	defer func() {
		if err := mock.ExpectationsWereMet(); err != nil {
			fmt.Printf("there were unfulfilled expectations: %s\n", err)
		}
	}()
	mock.ExpectBegin()
	mock.ExpectCommit()
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
	db, mock, err := sqlmock.New()
	if err != nil {
		panic(err)
	}
	defer db.Close()
	defer func() {
		if err := mock.ExpectationsWereMet(); err != nil {
			fmt.Printf("there were unfulfilled expectations: %s\n", err)
		}
	}()
	mock.ExpectBegin()
	mock.ExpectRollback()
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
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	defer func() {
		if err := mock.ExpectationsWereMet(); err != nil {
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
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	defer func() {
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
	}()

	mock.ExpectBegin()
	mock.ExpectCommit()

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
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	defer func() {
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
	}()

	mock.ExpectBegin()
	mock.ExpectRollback()
	mock.ExpectBegin()
	mock.ExpectCommit()

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
	db, mock, err := sqlmock.New()
	if err != nil {
		panic(err)
	}
	defer db.Close()
	defer func() {
		if err := mock.ExpectationsWereMet(); err != nil {
			fmt.Printf("there were unfulfilled expectations: %s\n", err)
		}
	}()

	mock.ExpectBegin()
	mock.ExpectRollback()
	mock.ExpectBegin()
	mock.ExpectRollback()
	mock.ExpectBegin()
	mock.ExpectCommit()

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
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	defer func() {
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
	}()

	mock.ExpectBegin()
	mock.ExpectRollback()
	mock.ExpectBegin()
	mock.ExpectRollback()
	mock.ExpectBegin()
	mock.ExpectRollback()

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
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	defer func() {
		if err := mock.ExpectationsWereMet(); err != nil {
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
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	defer func() {
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
	}()
	mock.ExpectBegin()
	mock.ExpectRollback()
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
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	defer func() {
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
	}()
	mock.ExpectBegin()
	mock.ExpectRollback()
	mock.ExpectBegin()
	mock.ExpectRollback()
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
