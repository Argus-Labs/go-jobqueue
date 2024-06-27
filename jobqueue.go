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
	JobStatusCompleted JobStatus = "complete"
)

// TODO: Use complete status for archiving completed jobs?

var ErrJobChannelClosed = errors.New("job channel is closed")

const DefaultFetchInterval = 100 * time.Millisecond

type JobQueue[T JobType] struct {
	db     *badger.DB
	ticker *time.Ticker
	wg     sync.WaitGroup
	logger zerolog.Logger

	isJobIDInQueue map[string]bool
	jobs           chan *job[T]

	// Options
	fetchInterval time.Duration
}

// NewJobQueue creates a new JobQueue with the specified database, name, and number
// of worker goroutines. It initializes the job queue, starts the worker goroutines,
// and returns the JobQueue instance and an error, if any.
func NewJobQueue[T JobType](dbPath string, name string, workers int, opts ...Option[T]) (*JobQueue[T], error) {
	if workers < 0 {
		return nil, errors.New("invalid number of workers")
	} else if workers == 0 {
		log.Warn().Msg("Number of workers is 0, jobs will not be automatically processed")
	}

	db, err := openDB(dbPath)
	if err != nil {
		return nil, err
	}

	jq := &JobQueue[T]{
		db:     db,
		wg:     sync.WaitGroup{},
		logger: log.With().Str("module", "JobQueue").Str("jobName", name).Logger(),

		isJobIDInQueue: make(map[string]bool),
		jobs:           make(chan *job[T]),

		fetchInterval: DefaultFetchInterval,
	}
	for _, opt := range opts {
		opt(jq)
	}

	jq.logger.Info().Msg("Starting job queue")

	// Initialize ticker based on the fetch interval
	jq.ticker = time.NewTicker(jq.fetchInterval)

	// Load jobs from BadgerDB
	go jq.fetchJobs()

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

	// Now that we've successfully processed the job, we can remove it from BadgerDB
	err := jq.db.Update(func(txn *badger.Txn) error {
		if err := txn.Delete([]byte(job.ID)); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		logger.Error().Err(err).Msg("Failed to remove completed job from db")
		return err
	}

	// Remove the job from the in-memory index
	delete(jq.isJobIDInQueue, job.ID)

	logger.Info().Msg("Job processed successfully")

	return nil
}

func (jq *JobQueue[T]) Stop() error {
	jq.logger.Info().Msg("Stopping job queue")

	// Stop jobs fetch from BadgerDB
	jq.logger.Debug().Msg("Stopping jobs fetch from BadgerDB")
	jq.ticker.Stop()

	// Close the channel to signal the workers to stop
	jq.logger.Debug().Msg("Closing job channel")
	close(jq.jobs)

	jq.logger.Debug().Msg("Waiting for workers to finish")
	jq.wg.Wait()

	// Close Badger DB connection
	jq.logger.Debug().Msg("Closing Badger DB connection")
	if err := jq.db.Close(); err != nil {
		jq.logger.Error().Err(err).Msg("Failed to close Badger DB connection")
		return err
	}

	jq.logger.Info().Msg("Job queue stopped successfully")

	return nil
}

func openDB(dbPath string) (*badger.DB, error) {
	opts := badger.DefaultOptions(dbPath)
	opts.Logger = nil

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open BadgerDB: %w", err)
	}

	return db, nil
}

// fetchJobs is a long-running goroutine that fetches jobs from BadgerDB and sends them to the worker channels.
func (jq *JobQueue[T]) fetchJobs() { //nolint:gocognit
	for range jq.ticker.C {
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

					if job.Status == JobStatusPending && !jq.isJobIDInQueue[job.ID] {
						select {
						case jq.jobs <- &job:
							jq.isJobIDInQueue[job.ID] = true
							jq.logger.Debug().Str("jobID", job.ID).Msg("New pending job found and sent to worker")
						default:
							return ErrJobChannelClosed
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
			if errors.Is(err, ErrJobChannelClosed) {
				// This is expected behavior when shutting down the job queue
				jq.logger.Warn().Msg("Founding pending jobs, but job channel is closed")
			} else {
				jq.logger.Error().Err(err).Msg("Error fetching jobs")
			}
		}
	}
}
