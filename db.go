package dbtools

import (
	"context"
	"runtime/debug"
	"time"

	"github.com/arsham/retry"
	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
)

// PGX is a concurrent-safe object that can retry a transaction on a
// pgxpool.Pool connection until it succeeds.
//
// Transaction method will try the provided functions one-by-one until all of
// them return nil, then commits the transaction. If any of the functions
// return any error other than a *retry.StopError, it will retry the
// transaction until the retry count is exhausted. If a running function
// returns a *retry.StopError, the transaction will be rolled-back and stops
// retrying. Tryouts will be stopped when the passed contexts are cancelled.
//
// If all attempts return errors, the last error is returned. If a
// *retry.StopError is returned, transaction is rolled back and the Err inside
// the *retry.StopError is returned. There will be delays between tries defined
// by the retry.DelayMethod and Delay duration.
//
// Any panic in functions will be wrapped in an error and will be counted as an
// error.
type PGX struct {
	pool Pool
	loop retry.Retry
}

// NewPGX returns an error if conn is nil. It sets the retry attempts to 1 if
// the value is less than 1. The retry strategy can be set either by providing
// a retry.Retry method or the individual components. See the ConfigFunc
// helpers.
func NewPGX(conn Pool, conf ...ConfigFunc) (*PGX, error) {
	if conn == nil {
		return nil, ErrEmptyDatabase
	}
	t := &PGX{
		pool: conn,
	}
	for _, fn := range conf {
		fn(t)
	}
	if t.loop.Attempts < 1 {
		t.loop.Attempts = 1
	}
	return t, nil
}

// Transaction returns an error if the connection is not set, or can't begin
// the transaction, or the after all retries, at least one of the fns returns
// an error, or the context is deadlined.
//
// It will wrap the commit/rollback methods if there are any. If in the last
// try any of the fns panics, it puts the stack trace of the panic in the error
// and returns.
//
// It stops retrying if any of the errors are wrapped in a *retry.StopError.
func (p *PGX) Transaction(ctx context.Context, fns ...func(pgx.Tx) error) error {
	if p.pool == nil {
		return ErrEmptyDatabase
	}

	return p.loop.Do(func() error {
		tx, err := p.pool.Begin(ctx)
		if err != nil {
			return errors.Wrap(err, "starting transaction")
		}

		for _, fn := range fns {
			select {
			case <-ctx.Done():
				err := &trError{err: ctx.Err()}
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				err.rollback = tx.Rollback(ctx)
				cancel()
				return &retry.StopError{Err: err}
			default:
			}

			var err error
			func() {
				defer func() {
					if r := recover(); r != nil {
						err = errors.Wrapf(errPanic, "%v\n%s", r, debug.Stack())
					}
				}()
				err = fn(tx)
			}()

			if err == nil {
				continue
			}
			if errors.Is(err, context.Canceled) {
				err = &retry.StopError{Err: err}
				ctx = context.Background()
			}
			if err != nil {
				err := &trError{err: err}
				err.rollback = tx.Rollback(ctx)
				return err
			}
		}
		return errors.Wrap(tx.Commit(ctx), "committing transaction")
	})
}
