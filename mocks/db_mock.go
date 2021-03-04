// Code generated by mockery v1.0.0. DO NOT EDIT.

package mocks

import (
	context "context"

	dbtools "github.com/arsham/dbtools"
	mock "github.com/stretchr/testify/mock"

	sql "database/sql"
)

// DB is an autogenerated mock type for the DB type
type DB struct {
	mock.Mock
}

// BeginTx provides a mock function with given fields: ctx, opts
func (_m *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (dbtools.Tx, error) {
	ret := _m.Called(ctx, opts)

	var r0 dbtools.Tx
	if rf, ok := ret.Get(0).(func(context.Context, *sql.TxOptions) dbtools.Tx); ok {
		r0 = rf(ctx, opts)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(dbtools.Tx)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, *sql.TxOptions) error); ok {
		r1 = rf(ctx, opts)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}