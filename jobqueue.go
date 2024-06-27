package jobqueue

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/goccy/go-json"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type JobStatus string

const (
	JobStatusPending   JobStatus = "pending"
	JobStatusCompleted JobStatus = "completed"
)

type JobQueue[T JobType] struct {
	db     *badger.DB
	wg     sync.WaitGroup
	jobs   chan *job[T]
	logger zerolog.Logger
}

func NewJobQueue[T JobType](db *badger.DB, name string, workers int) (*JobQueue[T], error) {
	jq := &JobQueue[T]{
		db:     db,
		wg:     sync.WaitGroup{},
		jobs:   make(chan *job[T]),
		logger: log.With().Str("module", "JobQueue").Str("jobName", name).Logger(),
	}

	// Start workers
	for i := 0; i < workers; i++ {
		jq.wg.Add(1)
		go jq.worker(i)
	}

	return jq, nil
}

func (jq *JobQueue[T]) Enqueue(payload T) (string, error) {
	job := newJob(payload)
	err := jq.db.Update(func(txn *badger.Txn) error {
		jobBytes, err := json.Marshal(job)
		if err != nil {
			return fmt.Errorf("failed to marshal job: %w", err)
		}

		err = txn.Set([]byte(job.ID), jobBytes)
		if err != nil {
			return fmt.Errorf("failed to store job: %w", err)
		}

		return nil
	})

	if err != nil {
		jq.logger.Error().Err(err).Str("jobID", job.ID).Msg("Failed to enqueue job")
		return "", err
	}

	jq.logger.Info().Str("jobID", job.ID).Msg("job enqueued successfully")
	return job.ID, nil
}

// worker processes jobs received from the job queue and logs any errors encountered.
func (jq *JobQueue[T]) worker(id int) {
	defer jq.wg.Done()

	logger := jq.logger.With().Int("worker", id).Logger()
	logger.Info().Msg("Worker started")

	// Worker stops running when the job channel is closed
	for job := range jq.jobs {
		err := jq.processJob(job)
		if err != nil {
			logger.Error().Err(err).Str("jobID", job.ID).Msg("Error processing job")
		}
	}

	logger.Info().Msg("Worker stopped")
}

// processJob processes a job and updates its status in the database.
func (jq *JobQueue[T]) processJob(job *job[T]) error {
	logger := jq.logger.With().Str("jobID", job.ID).Logger()
	if logger.GetLevel() == zerolog.DebugLevel {
		logger.Debug().Interface("jobPayload", job.Payload).Msg("Processing job")
	} else {
		logger.Info().Msg("Processing job")
	}

	if err := job.Process(); err != nil {
		return fmt.Errorf("failed to process job: %w", err)
	}

	err := jq.db.Update(func(txn *badger.Txn) error {
		jobBytes, err := json.Marshal(job)
		if err != nil {
			return fmt.Errorf("failed to marshal job: %w", err)
		}

		err = txn.Set([]byte(job.ID), jobBytes)
		if err != nil {
			return fmt.Errorf("failed to update job status: %w", err)
		}

		return nil
	})
	if err != nil {
		logger.Error().Err(err).Msg("Failed to update job status")
		return err
	}

	logger.Info().Msg("Job processed successfully")

	return nil
}

// Start starts the job queue and fetches jobs from the database.
func (jq *JobQueue[T]) Start() {
	jq.logger.Info().Msg("Starting job queue")
	go jq.fetchJobs()
}

func (jq *JobQueue[T]) Stop() error {
	jq.logger.Info().Msg("Stopping job queue")
	close(jq.jobs)

	// Wait for all workers to finish
	jq.wg.Wait()

	// Close Badger DB connection
	if err := jq.db.Close(); err != nil {
		jq.logger.Error().Err(err).Msg("Failed to close Badger DB connection")
		return err
	}

	return nil
}

// fetchJobs is a long-running goroutine that fetches jobs from BadgerDB and sends them to the worker channels.
func (jq *JobQueue[T]) fetchJobs() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		err := jq.db.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.PrefetchSize = 10
			it := txn.NewIterator(opts)
			defer it.Close()

			for it.Rewind(); it.Valid(); it.Next() {
				item := it.Item()
				err := item.Value(func(v []byte) error {
					var job job[T]
					if err := json.Unmarshal(v, &job); err != nil {
						return fmt.Errorf("failed to unmarshal job: %w", err)
					}

					if job.Status == JobStatusPending {
						select {
						case jq.jobs <- &job:
							jq.logger.Debug().Str("jobID", job.ID).Msg("job sent to worker")
						default:
							return errors.New("failed to send job to worker channel")
						}
					}

					return nil
				})
				if err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			jq.logger.Error().Err(err).Msg("Error fetching jobs")
		}
	}
}
