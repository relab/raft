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
				{Start: 1, End: 2},
			},
			[]*raftgorums.Interval{
				{Start: 1, End: 2},
			},
		},
		{
			"two disjoint",
			[]*raftgorums.Interval{
				{Start: 1, End: 2},
				{Start: 3, End: 4},
			},
			[]*raftgorums.Interval{
				{Start: 3, End: 4},
				{Start: 1, End: 2},
			},
		},
		{
			"two overlapping",
			[]*raftgorums.Interval{
				{Start: 1, End: 2},
				{Start: 2, End: 4},
			},
			[]*raftgorums.Interval{
				{Start: 1, End: 4},
			},
		},
		{
			"multiple intervals",
			[]*raftgorums.Interval{
				{Start: 2, End: 6},
				{Start: 3, End: 7},
				{Start: 7, End: 11},
				{Start: 8, End: 12},
				{Start: 10, End: 14},
				{Start: 15, End: 19},
				{Start: 30, End: 34},
			},
			[]*raftgorums.Interval{
				{Start: 30, End: 34},
				{Start: 15, End: 19},
				{Start: 2, End: 14},
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
