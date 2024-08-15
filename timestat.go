package jobqueue

import (
	"time"
)

type TimeStat struct {
	TotalTime time.Duration
	MinTime   time.Duration
	MaxTime   time.Duration
	Count     int
}

func (ts *TimeStat) AvgTime() time.Duration {
	if ts.Count == 0 {
		return 0
	}
	return ts.TotalTime / time.Duration(ts.Count)
}

func (ts *TimeStat) Reset() {
	ts.TotalTime = 0
	ts.MinTime = 0
	ts.MaxTime = 0
	ts.Count = 0
}

func (ts *TimeStat) RecordTime(duration time.Duration) {
	ts.TotalTime += duration
	ts.Count++
	if ts.MinTime == 0 || duration < ts.MinTime {
		ts.MinTime = duration
	}
	if duration > ts.MaxTime {
		ts.MaxTime = duration
	}
}

func (ts *TimeStat) String() string {
	return "tot " + ts.TotalTime.String() + " avg " + ts.AvgTime().String() + " min " + ts.MinTime.String() + " max " + ts.MaxTime.String()
}
