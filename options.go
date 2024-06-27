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
