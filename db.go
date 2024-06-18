package dbtools

import (
	"context"
	"fmt"
	"time"

	"github.com/arsham/retry/v3"
	"github.com/jackc/pgx/v5"
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
	pool        Pool
	loop        retry.Retry
	gracePeriod time.Duration
}

// New returns an error if conn is nil. It sets the retry attempts to 1 if the
// value is less than 1. The retry strategy can be set either by providing a
// retry.Retry method or the individual components. See the ConfigFunc helpers.
func New(conn Pool, conf ...ConfigFunc) (*PGX, error) {
	if conn == nil {
		return nil, ErrEmptyDatabase
	}
	obj := &PGX{
		pool:        conn,
		gracePeriod: 30 * time.Second,
		loop: retry.Retry{
			Attempts: 1,
			Delay:    300 * time.Millisecond,
			Method:   retry.IncrementalDelay,
		},
	}
	for _, fn := range conf {
		fn(obj)
	}
	if obj.loop.Attempts < 1 {
		obj.loop.Attempts = 1
	}

	return obj, nil
}

// Transaction returns an error if the connection is not set, or can't begin
// the transaction, or the after all retries, at least one of the fns returns
// an error, or the context is deadlined.
//
// It will wrap the commit/rollback methods if there are any. If in the last
// try any of the fns panics, it puts the stack trace of the panic in the error
// and returns.
//
// It stops retrying if any of the errors are wrapped in a *retry.StopError or
// when the context is cancelled.
func (p *PGX) Transaction(ctx context.Context, fns ...func(pgx.Tx) error) error {
	if p.pool == nil {
		return ErrEmptyDatabase
	}

	return p.loop.DoContext(ctx, func() error {
		tx, err := p.pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("starting transaction: %w", err)
		}

		for _, fn := range fns {
			var err error
			func() {
				defer func() {
					if r := recover(); r != nil {
						// In this case we want to rollback and panic so the
						// retry library can handle it.
						err = fmt.Errorf("%v", r)
						panic(p.rollbackWithErr(tx, err))
					}
				}()
				err = fn(tx)
			}()

			if err == nil {
				continue
			}

			return p.rollbackWithErr(tx, err)
		}

		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("committing transaction: %w", err)
		}

		return nil
	})
}

func (p *PGX) rollbackWithErr(tx pgx.Tx, err error) error {
	ctx, cancel := context.WithTimeout(context.Background(), p.gracePeriod)
	defer cancel()
	if er := tx.Rollback(ctx); er != nil {
		//nolint:wrapcheck // false positive.
		return fmt.Errorf("(rolling back transaction: %w): %w", er, err)
	}

	return err
}
