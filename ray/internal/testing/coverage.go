package testing

import (
	"fmt"
	"runtime/coverage"
)

var (
	// this variable will be set by go compiler via ldflags in run.sh
	coverageDir = ""
)

// WriteCoverageWhenTesting WriteCoverage writes the coverage info, for coverage testing.
func WriteCoverageWhenTesting() {
	if coverageDir != "" {
		err := coverage.WriteCountersDir(coverageDir)
		if err != nil {
			fmt.Printf("WriteCountersDir error: %v\n", err)
		}
	}
}
