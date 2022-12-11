package test

import (
	"mapreduce/util"
	"testing"
)

func TestJobNum(t *testing.T) {
	if util.Map != 0 {
		t.Error("Map JobNum should be 0")
	}

	if util.Reduce != 1 {
		t.Error("Reduce JobNum should be 1")
	}

	if util.Exit != 2 {
		t.Error("Exit JobNum should be 2")
	}
}

func TestJobState(t *testing.T) {
	if util.Waiting != 0 {
		t.Error("Waiting JobState should be 0")
	}

	if util.Running != 1 {
		t.Error("Running JobState should be 1")
	}

	if util.Stop != 2 {
		t.Error("Stop JobState should be 2")
	}

	if util.Error != 3 {
		t.Error("Error JobState should be 3")
	}
}
