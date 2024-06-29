package jobqueue

import (
	"fmt"
	"time"
)

type Option[T JobType] func(*JobQueue[T])

// WithFetchInterval sets the interval at which the job queue fetches jobs from BadgerDB.
func WithFetchInterval[T JobType](interval time.Duration) Option[T] {
	return func(jq *JobQueue[T]) {
		jq.logger.Debug().Msg(fmt.Sprintf("Fetch interval set to %vms", interval.Milliseconds()))
		jq.fetchInterval = interval
	}
}

// WithJobBufferSize sets the size of the job channel.
func WithJobBufferSize[T JobType](size int) Option[T] {
	return func(jq *JobQueue[T]) {
		jq.logger.Debug().Msg(fmt.Sprintf("Job buffer size set to %d", size))
		jq.jobs = make(chan *job[T], size)
	}
}
