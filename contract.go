// Package dbtools contains logic for database transaction, using the retry
// library.
package dbtools

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/arsham/retry/v3"
	"github.com/jackc/pgx/v5"
)

var (
	// ErrEmptyDatabase is returned when no database connection is set.
	ErrEmptyDatabase = errors.New("no database connection is set")

	errPanic = errors.New("function caused a panic")
)

// Pool is the contract for beginning a transaction with a pgxpool db
// connection.
//
//go:generate mockery --name Pool --filename pool_mock.go
type Pool interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

//nolint:unused,deadcode // only used for mocking.
//go:generate mockery --name pgxTx --filename pgx_tx_mock.go --structname PGXTx
type pgxTx interface {
	pgx.Tx
}

// Tx is a transaction began with sql.DB.
//
//go:generate mockery --name Tx --filename tx_mock.go
type Tx interface {
	Commit() error
	Exec(query string, args ...any) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	Prepare(query string) (*sql.Stmt, error)
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
	Query(query string, args ...any) (*sql.Rows, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	Rollback() error
	Stmt(stmt *sql.Stmt) *sql.Stmt
	StmtContext(ctx context.Context, stmt *sql.Stmt) *sql.Stmt
}

// A ConfigFunc function sets up a Transaction.
type ConfigFunc func(*PGX)

// WithRetry sets the retrier. The default retrier tries only once.
func WithRetry(r retry.Retry) ConfigFunc {
	return func(p *PGX) {
		p.loop = r
	}
}

// Retry sets the retry strategy. If you want to pass a Retry object you can
// use the WithRetry function instead.
func Retry(attempts int, delay time.Duration) ConfigFunc {
	return func(p *PGX) {
		p.loop.Attempts = attempts
		p.loop.Delay = delay
	}
}

// GracePeriod sets the context timeout when doing a rollback. This context
// needs to be different from the context user is giving as the user's context
// might be cancelled. The default value is 30s.
func GracePeriod(delay time.Duration) ConfigFunc {
	return func(p *PGX) {
		p.gracePeriod = delay
	}
}
