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

func TestNewQuorumSpec(t *testing.T) {
	minTests := []struct {
		name  string
		peers int
		n, q  int
	}{
		{"3 peers", 3, 2, 1},
		{"4 peers", 4, 3, 2},
		{"5 peers", 5, 4, 2},
		{"6 peers", 6, 5, 3},
		{"7 peers", 7, 6, 3},
	}

	for _, test := range minTests {
		t.Run(test.name, func(t *testing.T) {
			qs := NewQuorumSpec(test.peers)

			if qs.N != test.n {
				t.Errorf("got %d, want %d", qs.N, test.n)
			}

			if qs.Q != test.q {
				t.Errorf("got %d, want %d", qs.Q, test.q)
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
