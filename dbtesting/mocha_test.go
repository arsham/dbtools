package dbtesting_test

import (
	"bufio"
	"bytes"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/sclevine/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/arsham/dbtools/v3/dbtesting"
)

func TestTerminal(t *testing.T) {
	t.Parallel()
	t.Run("Start", testTerminalStart)
	t.Run("Specs", testTerminalSpecs)
}

func testTerminalStart(t *testing.T) {
	t.Run("Stdout", testTerminalStartStdout)
	t.Run("Buffer", testTerminalStartBuffer)
}

func testTerminalStartStdout(t *testing.T) {
	// This test replaces os.Stdout and should not be run in parallel with other
	// tests.
	original := os.Stdout
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = w
	defer func() {
		os.Stdout = original
	}()

	buf := &bytes.Buffer{}
	done := make(chan struct{})
	go func() {
		scanner := bufio.NewScanner(r)
		for scanner.Scan() {
			line := scanner.Text()
			buf.WriteString(line)
		}
		close(done)
	}()
	p := spec.Plan{Text: "satan"}
	m := &dbtesting.Mocha{}
	m.Start(t, p)
	w.Close()
	<-done
	content := buf.String()
	assert.Contains(t, content, "satan")
}

func testTerminalStartBuffer(t *testing.T) {
	t.Parallel()
	tcs := map[string]struct {
		plan spec.Plan
		want string
	}{
		"suite name": {
			spec.Plan{Text: "satan"},
			"satan",
		},
		"has random": {
			spec.Plan{HasRandom: true, Seed: 666},
			"666",
		},
		"has focus": {
			spec.Plan{HasFocus: true},
			"Focus",
		},
	}
	for name, tc := range tcs {
		tc := tc
		t.Run(name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			m := &dbtesting.Mocha{
				Out: buf,
			}
			m.Start(t, tc.plan)
			content := buf.String()
			assert.Contains(t, content, tc.want)
		})
	}
}

func testTerminalSpecs(t *testing.T) {
	t.Parallel()
	buf := &bytes.Buffer{}
	m := &dbtesting.Mocha{
		Out: buf,
	}
	specs := make(chan spec.Spec, 20)
	getSpec := func(i int, failed, skipped bool) spec.Spec {
		return spec.Spec{
			Text:    []string{"passed " + strconv.Itoa(i)},
			Skipped: skipped,
			Failed:  failed,
		}
	}
	// passed
	specs <- getSpec(1, false, false)
	specs <- getSpec(2, false, false)
	// failed
	specs <- getSpec(3, true, false)
	specs <- getSpec(4, true, false)
	specs <- getSpec(5, true, false)
	// skipped
	specs <- getSpec(6, false, true)
	specs <- getSpec(7, false, true)
	specs <- getSpec(8, false, true)
	specs <- getSpec(9, false, true)
	close(specs)

	m.Specs(t, specs)
	content := strings.ToLower(buf.String())
	tcs := map[string]string{
		"passed":  "passed: 2",
		"failed":  "failed: 3",
		"skipped": "skipped: 4",
	}
	for name, tc := range tcs {
		tc := tc
		t.Run(name, func(t *testing.T) {
			assert.Contains(t, content, tc)
		})
	}
}
