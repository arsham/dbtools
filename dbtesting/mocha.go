package dbtesting

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/sclevine/spec"
)

// Mocha prints spec reports in terminal.
type Mocha struct {
	Out  io.Writer // if not set it will print to stdout
	once sync.Once
}

func (m *Mocha) setup() {
	if m.Out == nil {
		m.Out = os.Stdout
	}
}

// Start prints some information when the suite is started.
func (m *Mocha) Start(_ *testing.T, plan spec.Plan) {
	m.once.Do(m.setup)
	fmt.Fprintln(m.Out, "Suite:", plan.Text)
	fmt.Fprintf(m.Out, "Total: %d | Focused: %d | Pending: %d\n", plan.Total, plan.Focused, plan.Pending)
	if plan.HasRandom {
		fmt.Fprintln(m.Out, "Random seed:", plan.Seed)
	}
	if plan.HasFocus {
		fmt.Fprintln(m.Out, "Focus is active.")
	}
}

// Specs prints information about specs' results while suite is running.
func (m *Mocha) Specs(_ *testing.T, specs <-chan spec.Spec) {
	m.once.Do(m.setup)
	var passed, failed, skipped int
	fs := "\033[31m" + "✘"
	ps := "\033[32m" + "✔"
	ss := "\033[32m" + "✱"
	for s := range specs {
		switch {
		case s.Failed:
			failed++
			fmt.Fprint(m.Out, fs)
		case s.Skipped:
			skipped++
			fmt.Fprint(m.Out, ss)
		default:
			passed++
			fmt.Fprint(m.Out, ps)
		}
		for i, txt := range s.Text {
			fmt.Fprintln(m.Out, strings.Repeat(" ", i*3), " ", txt)
		}
		fmt.Fprint(m.Out, "\033[0m")
	}
	fmt.Fprintf(m.Out, "\nPassed: %d | Failed: %d | Skipped: %d\n\n", passed, failed, skipped)
}
