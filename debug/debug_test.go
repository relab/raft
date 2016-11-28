package debug_test

import (
	"log"
	"testing"

	"github.com/relab/raft/debug"
)

func BenchmarkDebugPrintfOff(b *testing.B) {
	debug.SetVerbosity(debug.OFF)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		debug.Debugf("something happend (%d)", 42)
	}
}

func BenchmarkLogPrintfOffUsingBool(b *testing.B) {
	loggingEnabled := false
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if loggingEnabled {
			log.Printf("something happend (%d)", 42)
		}
	}
}
