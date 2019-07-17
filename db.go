package dbtools

import (
	"context"
	"database/sql"
	"io"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
)

// WithTransaction creates a transaction on db and uses it to call fn functions
// one by one. The first function that returns an error will cause the loop to
// stop and transaction to be rolled back.
func WithTransaction(db *sql.DB, fn ...func(*sql.Tx) error) error {
	tx, err := db.Begin()
	if err != nil {
		return errors.Wrap(err, "starting transaction")
	}
	for _, f := range fn {
		err := f(tx)
		if err != nil {
			e := errors.Wrap(tx.Rollback(), "rolling back transaction")
			return multierror.Append(err, e).ErrorOrNil()
		}
	}
	return errors.Wrap(tx.Commit(), "committing transaction")
}

// Retry calls fn for retries times until it returns nil. If retries is zero fn
// would not be called. It delays and retries if the function returns any
// errors. The fn function receives the current iteration as its argument.
func Retry(retries int, delay time.Duration, fn func(int) error) error {
	var err error
	for i := 0; i < retries; i++ {
		err = fn(i)
		switch err {
		case io.EOF, nil:
			return nil
		}
		time.Sleep(delay * time.Duration(i+1))
	}
	return err
}

// RetryTransaction combines WithTransaction and Retry calls. It stops the call
// if context is times out or cancelled.
func RetryTransaction(ctx context.Context, db *sql.DB, retries int, delay time.Duration, fn ...func(*sql.Tx) error) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return Retry(retries, delay, func(int) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		return WithTransaction(db, fn...)
	})
}
