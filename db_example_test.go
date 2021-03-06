package dbtools_test

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/arsham/dbtools"
	"github.com/arsham/retry"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/assert"
)

func ExampleNewTransaction() {
	// This setup tries the transaction only once.
	dbtools.NewTransaction(&exampleConn{})

	// This setup tries 100 times until succeeds. The delay is set to 10ms and
	// it uses the retry.IncrementalDelay method, which means every time it
	// increments the delay between retries with a jitter to avoid thunder herd
	// problem.
	dbtools.NewTransaction(&exampleConn{},
		dbtools.RetryCount(100),
		dbtools.RetryDelay(10*time.Millisecond),
		dbtools.DelayMethod(retry.IncrementalDelay),
	)
}

func ExampleTransaction_PGX() {
	tr, err := dbtools.NewTransaction(&exampleConn{})
	if err != nil {
		panic(err)
	}
	err = tr.PGX(context.Background(), func(pgx.Tx) error {
		fmt.Println("Running first query.")
		return nil
	}, func(pgx.Tx) error {
		fmt.Println("Running second query.")
		return nil
	})
	fmt.Printf("Transaction's error: %v", err)

	// Output:
	// Running first query.
	// Running second query.
	// Transaction's error: <nil>
}

func ExampleTransaction_PGX_retries() {
	tr, err := dbtools.NewTransaction(&exampleConn{}, dbtools.RetryCount(10))
	if err != nil {
		panic(err)
	}
	called := false
	err = tr.PGX(context.Background(), func(pgx.Tx) error {
		fmt.Println("Running first query.")
		return nil
	}, func(pgx.Tx) error {
		if !called {
			called = true
			fmt.Println("Second query error.")
			return assert.AnError
		}
		fmt.Println("Running second query.")
		return nil
	})
	fmt.Printf("Transaction's error: %v", err)

	// Output:
	// Running first query.
	// Second query error.
	// Running first query.
	// Running second query.
	// Transaction's error: <nil>
}

func ExampleTransaction_PGX_stopTrying() {
	// This example shows how to stop trying when we know an error is not
	// recoverable.
	tr, err := dbtools.NewTransaction(&exampleConn{},
		dbtools.RetryCount(100),
		dbtools.RetryDelay(time.Second),
	)
	if err != nil {
		panic(err)
	}
	err = tr.PGX(context.Background(), func(pgx.Tx) error {
		fmt.Println("Running first query.")
		return nil
	}, func(pgx.Tx) error {
		fmt.Println("Running second query.")
		return &retry.StopError{Err: assert.AnError}
	})
	fmt.Printf("Transaction returns my error: %t", strings.Contains(err.Error(), assert.AnError.Error()))

	// Output:
	// Running first query.
	// Running second query.
	// Transaction returns my error: true
}

func ExampleTransaction_PGX_panics() {
	tr, err := dbtools.NewTransaction(&exampleConn{}, dbtools.RetryCount(10))
	if err != nil {
		panic(err)
	}
	calls := 0
	err = tr.PGX(context.Background(), func(pgx.Tx) error {
		calls++
		fmt.Printf("Call #%d.\n", calls)
		if calls < 5 {
			panic("We have a panic!")
		}
		fmt.Println("All done.")
		return nil
	})
	fmt.Printf("Transaction's error: %v\n", err)
	fmt.Printf("Called %d times.\n", calls)

	// Output:
	// Call #1.
	// Call #2.
	// Call #3.
	// Call #4.
	// Call #5.
	// All done.
	// Transaction's error: <nil>
	// Called 5 times.
}
