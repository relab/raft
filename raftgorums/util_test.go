package raftgorums_test

import (
	"reflect"
	"testing"

	"github.com/relab/raft/raftgorums"
)

func TestMergeIntervals(t *testing.T) {
	var createIntervalsTests = []struct {
		name      string
		intervals []*raftgorums.Interval
		result    []*raftgorums.Interval
	}{
		{
			"empty",
			[]*raftgorums.Interval{},
			[]*raftgorums.Interval{},
		},
		{
			"single",
			[]*raftgorums.Interval{
				&raftgorums.Interval{1, 2},
			},
			[]*raftgorums.Interval{
				&raftgorums.Interval{1, 2},
			},
		},
		{
			"two disjoint",
			[]*raftgorums.Interval{
				&raftgorums.Interval{1, 2},
				&raftgorums.Interval{3, 4},
			},
			[]*raftgorums.Interval{
				&raftgorums.Interval{3, 4},
				&raftgorums.Interval{1, 2},
			},
		},
		{
			"two overlapping",
			[]*raftgorums.Interval{
				&raftgorums.Interval{1, 2},
				&raftgorums.Interval{2, 4},
			},
			[]*raftgorums.Interval{
				&raftgorums.Interval{1, 4},
			},
		},
		{
			"multiple intervals",
			[]*raftgorums.Interval{
				&raftgorums.Interval{2, 6},
				&raftgorums.Interval{3, 7},
				&raftgorums.Interval{7, 11},
				&raftgorums.Interval{8, 12},
				&raftgorums.Interval{10, 14},
				&raftgorums.Interval{15, 19},
				&raftgorums.Interval{30, 34},
			},
			[]*raftgorums.Interval{
				&raftgorums.Interval{30, 34},
				&raftgorums.Interval{15, 19},
				&raftgorums.Interval{2, 14},
			},
		},
	}

	for _, test := range createIntervalsTests {
		t.Run(test.name, func(t *testing.T) {
			result := raftgorums.MergeIntervals(test.intervals)

			if !reflect.DeepEqual(result, test.result) {
				k := len(result)
				if len(test.result) > k {
					k = len(test.result)
				}
				for i := 0; i < k; i++ {
					switch {
					case i < len(result) && i < len(test.result):
						if !reflect.DeepEqual(result[i], test.result[i]) {
							t.Errorf("got %+v, want %+v", result[i], test.result[i])
						}
					case i < len(result):
						t.Errorf("got %+v, want %+v", result[i], "<none>")
					case i < len(test.result):
						t.Errorf("got %+v, want %+v", "<none>", test.result[i])
					}
				}
			}
		})
	}
}
