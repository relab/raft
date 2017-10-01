package raftgorums

import (
	"math/rand"
	"testing"
	"time"
)

func TestMinMax(t *testing.T) {
	minTests := []struct {
		name       string
		x, y       uint64
		rmin, rmax uint64
	}{
		{"0,0", 0, 0, 0, 0},
		{"0,1", 0, 1, 0, 1},
		{"1,0", 1, 0, 0, 1},
	}

	for _, test := range minTests {
		t.Run(test.name, func(t *testing.T) {
			rmin := min(test.x, test.y)
			rmax := max(test.x, test.y)

			if rmin != test.rmin {
				t.Errorf("got %v, want %v", rmin, test.rmin)
			}

			if rmax != test.rmax {
				t.Errorf("got %v, want %v", rmax, test.rmax)
			}
		})
	}
}

func TestRandomTimeout(t *testing.T) {
	rand.Seed(99)

	base := time.Microsecond

	for i := 0; i < 999; i++ {
		rnd := randomTimeout(base)

		if rnd < base || rnd > 2*base {
			t.Errorf("random timeout out of bounds: %d < %d < %d", base, rnd, 2*base)
		}
	}
}
