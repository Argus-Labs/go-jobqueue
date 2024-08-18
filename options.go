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

// how many jobs at once are retrieved from the DB in a single fetch operation
func WithJobsPerFetch[T any](count int) Option[T] {
	return func(jq *JobQueue[T]) {
		jq.logger.Debug().Msg(fmt.Sprintf("Jobs per fetch set to %d", count))
		jq.jobsPerFetch = count
	}
}

// WithInmemDB uses an in-memory BadgerDB instead of a persistent one.
// Useful for testing, but provides no durability guarantees.
// if we previously called UseMongoDB, we will warn and ignore this option.
func WithInMemDB[T any]() Option[T] {
	return func(jq *JobQueue[T]) {
		if jq.dbUseMongo {
			jq.logger.Warn().Msg("Ignoring WithInMemDB option, not compatible with UseMongoDB option")
		} else if jq.dbUseBadger {
			jq.logger.Warn().Msg("Ignoring WithInMemDB option, not compatible with UseBadgerDB option")
		} else {
			jq.logger.Debug().Msg("Using Badger In-Memory DB for Job Queue DB")
			jq.dbInMemory = true
		}
	}
}

// WithMongoDB sets the JobQueue to use MongoDB instead of BadgerDB.
// if WithInMemDB was previously called, we will warn and ignore this option.
func WithMongoDB[T any](uri string) Option[T] {
	return func(jq *JobQueue[T]) {
		if jq.dbInMemory {
			jq.logger.Warn().Msg("Ignoring WithMongoDB option, not compatible with WithInMemDB option")
		} else if jq.dbUseBadger {
			jq.logger.Warn().Msg("Ignoring WitMongoDB option, not compatible with WithBadgerDB option")
		} else {
			jq.logger.Debug().Msg(fmt.Sprintf("Using Mongo DB at %s for Job Queue DB", uri))
			jq.dbPath = uri
			jq.dbUseMongo = true
		}
	}
}

// WithBadgerDB sets the JobQueue to use BadgerDB instead of MongoDB.
// if WithInMemDB or WithBadgerDB was previously called, we will warn and ignore this option.
func WithBadgerDB[T any](path string) Option[T] {
	return func(jq *JobQueue[T]) {
		if jq.dbInMemory {
			jq.logger.Warn().Msg("Ignoring WithBadgerDB option, not compatible with WithInMemDB option")
		} else if jq.dbUseMongo {
			jq.logger.Warn().Msg("Ignoring WithBadgerDB option, not compatible with WithMongoDB option")
		} else {
			jq.logger.Debug().Msg(fmt.Sprintf("Using Badger DB at %s for Job Queue DB", path))
			jq.dbPath = path
			jq.dbUseBadger = true
		}
	}
}
