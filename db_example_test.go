package dbtools_test

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/arsham/dbtools/v3"
	"github.com/arsham/retry/v3"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
)

func ExampleNewPGX() {
	// This setup tries the transaction only once.
	dbtools.NewPGX(&exampleConn{})

	// This setup tries 100 times until succeeds. The delay is set to 10ms and
	// it uses the retry.IncrementalDelay method, which means every time it
	// increments the delay between retries with a jitter to avoid thunder herd
	// problem.
	dbtools.NewPGX(&exampleConn{},
		dbtools.Retry(10, 100*time.Millisecond),
	)
	// Output:
}

func ExamplePGX_Transaction() {
	tr, err := dbtools.NewPGX(&exampleConn{})
	if err != nil {
		panic(err)
	}
	err = tr.Transaction(context.Background(), func(pgx.Tx) error {
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

func ExamplePGX_Transaction_retries() {
	tr, err := dbtools.NewPGX(&exampleConn{}, dbtools.Retry(10, 100*time.Millisecond))
	if err != nil {
		panic(err)
	}
	called := false
	err = tr.Transaction(context.Background(), func(pgx.Tx) error {
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

func ExamplePGX_Transaction_stopTrying() {
	// This example shows how to stop trying when we know an error is not
	// recoverable.
	tr, err := dbtools.NewPGX(&exampleConn{},
		dbtools.Retry(10, time.Second),
	)
	if err != nil {
		panic(err)
	}
	err = tr.Transaction(context.Background(), func(pgx.Tx) error {
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

func ExamplePGX_Transaction_panics() {
	tr, err := dbtools.NewPGX(&exampleConn{}, dbtools.Retry(10, 100*time.Millisecond))
	if err != nil {
		panic(err)
	}
	calls := 0
	err = tr.Transaction(context.Background(), func(pgx.Tx) error {
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
