package dbtesting_test

import (
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/arsham/dbtools/dbtesting"
)

func TestOkValue(t *testing.T) {
	t.Parallel()
	tcs := map[string]interface{}{
		"nil":        nil,
		"int":        666,
		"float":      66.6,
		"string":     "satan",
		"byte slice": []byte("devil"),
	}
	for name, tc := range tcs {
		tc := tc
		t.Run(name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("got %v, want nil", err)
			}
			defer db.Close()
			defer func() {
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Errorf("there were unfulfilled expectations: %s", err)
				}
			}()
			mock.ExpectExec("INSERT INTO life .+").
				WithArgs(dbtesting.OkValue).
				WillReturnResult(sqlmock.NewResult(1, 1))
			_, err = db.Exec("INSERT INTO life (name) VALUE ($1)", tc)
			if err != nil {
				t.Errorf("got %v, want nil", err)
			}
		})
	}
}

func ExampleOkValue() {
	db, mock, err := sqlmock.New()
	if err != nil {
		panic(err)
	}
	defer db.Close()
	defer func() {
		if err := mock.ExpectationsWereMet(); err != nil {
			fmt.Printf("there were unfulfilled expectations: %s", err)
		}
	}()
	mock.ExpectExec("INSERT INTO life .+").
		WithArgs(dbtesting.OkValue).
		WillReturnResult(sqlmock.NewResult(1, 1))
	_, err = db.Exec("INSERT INTO life (name) VALUE ($1)", 666)
	fmt.Println("Error:", err)

	// Output:
	// Error: <nil>
}

func TestValueRecorder(t *testing.T) {
	t.Run("Record", testValueRecorderRecord)
	t.Run("RecordPanic", testValueRecorderRecordPanic)
	t.Run("For", testValueRecorderFor)
	t.Run("ForPanic", testValueRecorderForPanic)
	t.Run("Value", testValueRecorderValue)
	t.Run("ValuePanic", testValueRecorderValuePanic)
}

func testValueRecorderRecord(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("got %v, want nil", err)
	}
	defer db.Close()
	defer func() {
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
	}()
	defer func() {
		if e := recover(); e != nil {
			t.Errorf("didn't expect to panic: %v", e)
		}
	}()
	rec := dbtesting.NewValueRecorder()
	mock.ExpectExec(".+").
		WithArgs(rec.Record("satan")).
		WillReturnResult(sqlmock.NewResult(1, 1))
	_, err = db.Exec("query", float64(66.6))
	if err != nil {
		t.Fatalf("got %v, want nil", err)
	}
	got := rec.Value("satan")
	if v, ok := got.(float64); !ok || v != 66.6 {
		t.Errorf("%+v: got %f, want %f", got, v, 66.6)
	}
}

func testValueRecorderRecordPanic(t *testing.T) {
	t.Parallel()
	defer func() {
		paniced := false
		if e := recover(); e != nil {
			paniced = true
		}
		if !paniced {
			t.Error("did not panic")
		}
	}()
	rec := dbtesting.NewValueRecorder()
	rec.Record("god")
	rec.Record("god")
}

func testValueRecorderFor(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("got %v, want nil", err)
	}
	defer db.Close()
	defer func() {
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
	}()
	defer func() {
		if e := recover(); e != nil {
			t.Errorf("didn't expect to panic: %v", e)
		}
	}()
	rec := dbtesting.NewValueRecorder()
	mock.ExpectExec("query1").
		WithArgs(rec.Record("satan")).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("query2").
		WithArgs(rec.For("satan")).
		WillReturnResult(sqlmock.NewResult(1, 1))
	_, err = db.Exec("query1", float64(66.6))
	if err != nil {
		t.Fatalf("got %v, want nil", err)
	}
	_, err = db.Exec("query2", float64(66.6))
	if err != nil {
		t.Fatalf("got %v, want nil", err)
	}
}

func testValueRecorderForPanic(t *testing.T) {
	t.Parallel()
	defer func() {
		paniced := false
		if e := recover(); e != nil {
			paniced = true
		}
		if !paniced {
			t.Error("did not panic")
		}
	}()
	rec := dbtesting.NewValueRecorder()
	rec.For("god")
}

func testValueRecorderValue(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("got %v, want nil", err)
	}
	defer db.Close()
	defer func() {
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
	}()
	defer func() {
		if e := recover(); e != nil {
			t.Errorf("didn't expect to panic: %v", e)
		}
	}()
	val := float64(66.6)
	rec := dbtesting.NewValueRecorder()
	mock.ExpectExec("query").
		WithArgs(rec.Record("satan")).
		WillReturnResult(sqlmock.NewResult(1, 1))
	_, err = db.Exec("query", val)
	if err != nil {
		t.Fatalf("got %v, want nil", err)
	}
	got := rec.Value("satan").(float64)
	if got != val {
		t.Errorf("got %f, want %f", got, val)
	}
}

func testValueRecorderValuePanic(t *testing.T) {
	t.Parallel()
	defer func() {
		paniced := false
		if e := recover(); e != nil {
			paniced = true
		}
		if !paniced {
			t.Error("did not panic")
		}
	}()
	rec := dbtesting.NewValueRecorder()
	rec.Value("god")
}

func ExampleValueRecorder() {
	db, mock, err := sqlmock.New()
	if err != nil {
		panic(err)
	}
	defer db.Close()
	defer func() {
		if err := mock.ExpectationsWereMet(); err != nil {
			fmt.Printf("there were unfulfilled expectations: %s", err)
		}
	}()
	rec := dbtesting.NewValueRecorder()
	mock.ExpectExec("INSERT INTO life .+").
		WithArgs(rec.Record("truth")).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO reality .+").
		WithArgs(rec.For("truth")).
		WillReturnResult(sqlmock.NewResult(1, 1))

	// pretend the following query happens in another package and the argument is
	// totally random.
	_, err = db.Exec("INSERT INTO life (name) VALUE ($1)", 666)
	fmt.Println("Error:", err)

	// say we don't have access to the value and we don't know what value would be
	// passed, but it is important the value is the same as the logic has to pass.

	_, err = db.Exec("INSERT INTO reality (name) VALUE ($1)", 666)
	fmt.Println("Error:", err)

	fmt.Printf("got recorded value: %d", rec.Value("truth"))

	// Output:
	// Error: <nil>
	// Error: <nil>
	// got recorded value: 666
}

func ExampleValueRecorder_value() {
	db, mock, err := sqlmock.New()
	if err != nil {
		panic(err)
	}
	defer db.Close()
	defer func() {
		if err := mock.ExpectationsWereMet(); err != nil {
			fmt.Printf("there were unfulfilled expectations: %s", err)
		}
	}()
	rec := dbtesting.NewValueRecorder()
	mock.ExpectExec("INSERT INTO life .+").
		WithArgs(rec.Record("meaning")).
		WillReturnResult(sqlmock.NewResult(1, 1))

	_, err = db.Exec("INSERT INTO life (name) VALUE ($1)", 42)
	fmt.Println("Error:", err)
	fmt.Printf("Meaning of life: %d", rec.Value("meaning").(int64))

	// Output:
	// Error: <nil>
	// Meaning of life: 42
}
