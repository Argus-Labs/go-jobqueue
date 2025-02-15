package jobqueue

import (
	"fmt"
	"time"
)

type Option[T any] func(*JobQueue[T])

// WithFetchInterval sets the interval at which the job queue fetches jobs from BadgerDB.
func WithFetchInterval[T any](interval time.Duration) Option[T] {
	return func(jq *JobQueue[T]) {
		jq.logger.Debug().Msg(fmt.Sprintf("Fetch interval set to %vms", interval.Milliseconds()))
		jq.fetchInterval = interval
	}
}

// WithJobBufferSize sets the size of the job channel.
func WithJobBufferSize[T any](size int) Option[T] {
	return func(jq *JobQueue[T]) {
		jq.logger.Debug().Msg(fmt.Sprintf("Job buffer size set to %d", size))
		jq.jobs = make(chan *job[T], size)
	}
}

// WithInmemDB uses an in-memory BadgerDB instead of a persistent one.
// Useful for testing, but provides no durability guarantees.
func WithInmemDB[T any]() Option[T] {
	return func(jq *JobQueue[T]) {
		jq.dbInMemory = true
	}
}
