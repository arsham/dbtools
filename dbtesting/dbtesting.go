// Package dbtesting provides handy tools for using with databases.
package dbtesting

import (
	"database/sql/driver"
	"reflect"

	"github.com/DATA-DOG/go-sqlmock"
)

// OkValue is used for sqlmock package for when the checks should always return
// true.
var OkValue = okValue{}

type okValue struct{}

// Match always returns true.
func (okValue) Match(driver.Value) bool { return true }

// ValueRecorder records the values when they are seen and compares them when
// they are asked. You can create a new ValueRecorder with NewValueRecorder
// function. Values should have one Record call and zero or more For calls.
type ValueRecorder interface {
	// Record records the value of the value the first time it sees it. It panics
	// if the value is already been recorded.
	Record(name string) sqlmock.Argument
	// For reuses the value in the query. It panics if the value is not been
	// recorded.
	For(name string) sqlmock.Argument
	// Value returns the recorded value of the item. It panics if the value is not
	// been recorded.
	Value(name string) any
}

// NewValueRecorder returns a fresh ValueRecorder instance.
func NewValueRecorder() ValueRecorder {
	return make(valueRecorder)
}

type value struct {
	val   any
	valid bool
}

func (v *value) Match(val driver.Value) bool {
	if !v.valid {
		v.val = val
		v.valid = true
		return true
	}
	return reflect.DeepEqual(val, v.val)
}

type valueRecorder map[string]*value

// Record records the value of the value the first time it sees it. It panics if
// the value is already been recorded.
func (v valueRecorder) Record(s string) sqlmock.Argument {
	_, ok := v[s]
	if ok {
		panic(s + " recorded twice")
	}
	v[s] = &value{}
	return v[s]
}

// For reuses the value in the query. It panics if the value is not been
// recorded.
func (v valueRecorder) For(s string) sqlmock.Argument {
	id, ok := v[s]
	if !ok || id == nil {
		panic(s + " not recorded yet")
	}
	return id
}

// Value returns the recorded value of the item. It panics if the value is not
// been recorded.
func (v valueRecorder) Value(s string) any {
	id, ok := v[s]
	if !ok || id == nil {
		panic(s + " not recorded yet")
	}
	return id.val
}
