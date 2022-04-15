package dbtools

import (
	"context"
	"runtime/debug"
	"time"

	"github.com/arsham/retry"
	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
)

// Transaction is a concurrent-safe object that can retry a transaction on
// either a sql.DB or a pgxpool connection until it succeeds.
//
// DB and PGX will try transaction functions one-by-one until all of them return
// nil, then commits the transaction. If any of the transactions return any
// error other than retry.StopError, it will retry the transaction until the
// retry count is exhausted. If a running function returns a retry.StopError,
// the transaction will be rolled-back and would stop retrying. Tryouts will be
// stopped when the passed contexts are cancelled.
//
// If all attempts return errors, the last error is returned. If a
// retry.StopError is returned, transaction is rolled back and the Err inside
// the retry.StopError is returned. There will be delays between tries defined
// by the retry.DelayMethod and Delay duration.
//
// Any panic in transactions will be wrapped in an error and will be counted as
// an error, either being retried or returned.
//
// It's an error to invoke the methods without their respective connections are
// set.
type Transaction struct {
	method  retry.DelayMethod
	pool    Pool
	loop    retry.Retry
	delay   time.Duration
	retries int
}

// NewTransaction returns an error if conn is not a DB, Pool, or *sql.DB
// connection.
func NewTransaction(conn interface{}, conf ...ConfigFunc) (*Transaction, error) {
	if conn == nil {
		return nil, ErrEmptyDatabase
	}
	t := &Transaction{}
	switch db := conn.(type) {
	case Pool:
		t.pool = db
	default:
		return nil, ErrEmptyDatabase
	}

	for _, fn := range conf {
		fn(t)
	}
	if t.retries < 1 {
		t.retries = 1
	}
	t.loop = retry.Retry{
		Attempts: t.retries,
		Delay:    t.delay,
		Method:   t.method,
	}
	return t, nil
}

// PGX returns an error if a pgxpool connection is not set.
func (t *Transaction) PGX(ctx context.Context, transactions ...func(pgx.Tx) error) error {
	if t.pool == nil {
		return ErrEmptyDatabase
	}
	return t.loop.Do(func() error {
		tx, err := t.pool.Begin(ctx)
		if err != nil {
			return errors.Wrap(err, "starting transaction")
		}
		for _, fn := range transactions {
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
