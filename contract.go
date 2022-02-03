package dbtools

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/arsham/retry"
	"github.com/jackc/pgx/v4"
)

var (
	// ErrEmptyDatabase is returned when no database connection is set.
	ErrEmptyDatabase = errors.New("no database connection is set")

	errPanic = errors.New("function caused a panic")
)

// DB is the contract for beginning a transaction with a *sql.DB object.
//go:generate mockery --name DB --filename db_mock.go
type DB interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (Tx, error)
}

// Pool is the contract for beginning a transaction with a pgxpool db
// connection.
//go:generate mockery --name Pool --filename pool_mock.go
type Pool interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

//nolint:unused // only used for mocking.
//go:generate mockery --name pgxTx --filename pgx_tx_mock.go --structname PGXTx
type pgxTx interface {
	pgx.Tx
}

// Tx is a transaction began with sql.DB.
//go:generate mockery --name Tx --filename tx_mock.go
type Tx interface {
	Commit() error
	Exec(query string, args ...interface{}) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	Prepare(query string) (*sql.Stmt, error)
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
	Rollback() error
	Stmt(stmt *sql.Stmt) *sql.Stmt
	StmtContext(ctx context.Context, stmt *sql.Stmt) *sql.Stmt
}

// A ConfigFunc function sets up a Transaction.
type ConfigFunc func(*Transaction)

// RetryCount defines a transaction should be tried n times. If n is 0, it will
// be set as 1.
func RetryCount(n int) ConfigFunc {
	return func(t *Transaction) {
		t.retries = n
	}
}

// RetryDelay is the amount of delay between each unsuccessful tries. Set
// DelayMethod for the method of delay duration.
func RetryDelay(d time.Duration) ConfigFunc {
	return func(t *Transaction) {
		t.delay = d
	}
}

// DelayMethod decides how to delay between each tries. Default is
// retry.StandardDelay.
func DelayMethod(m retry.DelayMethod) ConfigFunc {
	return func(t *Transaction) {
		t.method = m
	}
}

type dbWrapper struct {
	db *sql.DB
}

func (d *dbWrapper) BeginTx(ctx context.Context, opts *sql.TxOptions) (Tx, error) {
	return d.db.BeginTx(ctx, opts)
}
