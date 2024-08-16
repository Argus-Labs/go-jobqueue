package jobqueue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/loov/hrtime"
	"github.com/puzpuzpuz/xsync/v3"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"go.uber.org/atomic"
)

type JobStatus string

const (
	JobStatusPending   JobStatus = "pending"
	JobStatusCompleted JobStatus = "complete"
)

// TODO: Use complete status for archiving completed jobs?

var errJobChannelFull = errors.New("job channel is closed")

const defaultFetchInterval = 100 * time.Millisecond
const defaultJobBufferSize = 1000
const defaultJobsPerFetch = 10

type JobQueue[T any] struct {
	db         JobQueueDb[T]
	dbPath     string
	dbInMemory bool

	wg      sync.WaitGroup
	logger  zerolog.Logger
	cancel  context.CancelFunc
	handler func(JobContext, T) error

	isJobIDInQueue *xsync.MapOf[uint64, bool]
	jobs           chan *job[T]

	// Options
	fetchInterval time.Duration
	jobsPerFetch  int

	// Stats
	statsLock sync.Mutex // protects the stats below

	// job stats
	jobRunTime    TimeStat // stats on time that it takes to run a job (across all workers)
	jobQueuedTime TimeStat // stats on how much time a job sits in the queue before being processed

	// queue stats
	busyTime      TimeStat // stats on time that the queue actively processing jobs
	idleTime      TimeStat // stats on how much time the queue is empty between jobs being processed
	jobsProcessed int
	jobsEnqueued  int
	jobsFailed    int
	jobsSucceeded int

	busyWorkerCount   atomic.Int32
	busyStateChangeAt atomic.Time
	queueIsIdle       atomic.Bool
}

// New creates a new JobQueue with the specified database, name, and number
// of worker goroutines. It initializes the job queue, starts the worker goroutines,
// and returns the JobQueue instance and an error, if any.
func New[T any](
	dbPath string, name string, workers int, handler func(JobContext, T) error, opts ...Option[T],
) (*JobQueue[T], error) {
	if workers < 0 {
		return nil, errors.New("invalid number of workers")
	} else if workers == 0 {
		log.Warn().Msg("Number of workers is 0, jobs will not be automatically processed")
	}

	jq := &JobQueue[T]{
		db:         nil,
		dbPath:     dbPath,
		dbInMemory: false,

		wg:      sync.WaitGroup{},
		logger:  log.With().Str("module", "JobQueue").Str("jobName", name).Logger(),
		cancel:  nil,
		handler: handler,

		isJobIDInQueue: xsync.NewMapOf[uint64, bool](),
		jobs:           make(chan *job[T], defaultJobBufferSize),

		fetchInterval: defaultFetchInterval,
		jobsPerFetch:  defaultJobsPerFetch,

		statsLock:     sync.Mutex{},
		jobRunTime:    TimeStat{},
		jobQueuedTime: TimeStat{},
		busyTime:      TimeStat{}, // wall time, not CPU time
		idleTime:      TimeStat{}, // wall time, not CPU time
		jobsProcessed: 0,
		jobsEnqueued:  0,
		jobsFailed:    0,
		jobsSucceeded: 0,
	}

	jq.busyWorkerCount.Store(0)
	jq.busyStateChangeAt.Store(time.Now())
	jq.queueIsIdle.Store(true)

	for _, opt := range opts {
		opt(jq)
	}

	// TODO: figure out better way to do options for JobQueue DB
	db := NewJobQueueDbBadger[T](jq.dbInMemory) // hardcoding BadgerDB for now. Add option for other DBs later

	err := db.Open(dbPath, name)
	if err != nil {
		return nil, err
	}
	jq.db = db

	jq.logger.Info().Msg("Starting job queue")

	ctx, cancel := context.WithCancel(context.Background())
	jq.cancel = cancel

	// Load jobs from JobQueue DB
	go jq.pollJobs(ctx)

	// Start workers
	for i := 0; i < workers; i++ {
		jq.wg.Add(1)
		go jq.worker(i)
	}

	return jq, nil
}

func (jq *JobQueue[T]) Enqueue(payload T) (uint64, error) {
	// TODO: simplify by getting and setting the job ID when we add it, rather than on creation
	id, err := jq.db.GetNextJobId()
	if err != nil {
		return 0, fmt.Errorf("failed to get next job id: %w", err)
	}

	// Create a new job and store it in queue's database
	job := newJob(id, payload)
	_, err = jq.db.AddJob(job)
	if err != nil {
		jq.logger.Error().Err(err).Uint64("jobID", job.ID).Msg("Failed to enqueue job")
		return 0, err
	}
	jq.statsLock.Lock()
	jq.jobsEnqueued++
	jq.statsLock.Unlock()

	jq.logger.Info().Uint64("jobID", job.ID).Msg("job enqueued successfully")
	return job.ID, nil
}

// worker processes jobs received from the job queue and logs any errors encountered.
func (jq *JobQueue[T]) worker(id int) {
	defer jq.wg.Done()

	logger := jq.logger.With().Int("worker", id).Logger()
	logger.Info().Msg("Worker started")

	// Worker stops running when the job channel is closed
	for job := range jq.jobs {

		wasIdle := jq.queueIsIdle.Swap(false)
		jq.busyWorkerCount.Inc()
		if wasIdle {
			timeSpentInState := time.Since(jq.busyStateChangeAt.Load())
			jq.busyStateChangeAt.Store(time.Now())
			jq.idleTime.RecordTime(timeSpentInState)
			logger.Debug().Dur("timeIdle(ms)", timeSpentInState).Msg("*** Queue now busy *** ")
		}

		err := jq.processJob(job, id)

		if jq.busyWorkerCount.Dec() == 0 {
			wasIdle := jq.queueIsIdle.Swap(true)
			if !wasIdle {
				timeSpentInState := time.Since(jq.busyStateChangeAt.Load())
				jq.busyStateChangeAt.Store(time.Now())
				jq.busyTime.RecordTime(timeSpentInState)
				logger.Debug().Dur("timeBusy(ms)", timeSpentInState).Msg("*** Queue now idle *** ")
			}
		}

		if err != nil {
			logger.Error().Err(err).Uint64("jobID", job.ID).Msg("Error processing job")
		}
	}

	logger.Info().Msg("Worker stopped")
}

// processJob processes a job and updates its status in the database.
func (jq *JobQueue[T]) processJob(job *job[T], worker int) error {
	logger := jq.logger.With().Uint64("jobID", job.ID).Logger()
	if logger.GetLevel() == zerolog.DebugLevel {
		logger.Debug().Interface("jobPayload", job.Payload).Int("worker", worker).Msg("Processing job")
	} else {
		logger.Info().Int("worker", worker).Msg("Processing job")
	}

	queuedTime := time.Since(job.CreatedAt)
	startTime := hrtime.Now()
	err := job.Process(jq.handler)
	runTime := hrtime.Since(startTime)
	jq.statsLock.Lock()
	jq.jobsProcessed++
	jq.jobRunTime.RecordTime(runTime)
	jq.jobQueuedTime.RecordTime(queuedTime)
	if err != nil {
		jq.jobsFailed++
		jq.statsLock.Unlock()
		return fmt.Errorf("failed to process job: %w", err)
	}
	jq.jobsSucceeded++
	jq.statsLock.Unlock()
	logger.Info().Msg("Job processed successfully")

	// Now that we've successfully processed the job, we can remove it from JobQueue DB
	jq.logger.Debug().Uint64("jobID", job.ID).Int("worker", worker).Msg("Removing job from JobQueue DB")
	err = jq.db.DeleteJob(job.ID)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to remove completed job from db")
		return err
	}

	// Remove the job from the in-memory index
	jq.logger.Debug().Uint64("jobID", job.ID).Int("worker", worker).Msg("Removing job from in-memory index")
	jq.isJobIDInQueue.Delete(job.ID)

	return nil
}

func (jq *JobQueue[T]) Stop() error {
	jq.logger.Info().Msg("Stopping job queue")

	// Stop jobs fetch from JobQueue DB
	jq.logger.Debug().Msg("Stopping jobs fetch from JobQueue DB")
	jq.cancel()

	// Close the channel to signal the workers to stop
	jq.logger.Debug().Msg("Closing job channel")
	close(jq.jobs)

	jq.logger.Debug().Msg("Waiting for workers to finish")
	jq.wg.Wait()

	// Close JobQueue DB connection
	jq.logger.Debug().Msg("Closing JobQueue DB connection")
	if err := jq.db.Close(); err != nil {
		jq.logger.Error().Err(err).Msg("Failed to close JobQueue DB connection")
		return err
	}

	if jq.jobsEnqueued+jq.jobsProcessed > 0 {
		jq.logger.Info().
			Int("jobsProcessed", jq.jobsProcessed).
			Int("jobsEnqueued", jq.jobsEnqueued).
			Int("jobsFailed", jq.jobsFailed).
			Int("jobsSucceeded", jq.jobsSucceeded).
			Str("jobRunTime", jq.jobRunTime.String()).
			Str("jobQueuedTime", jq.jobQueuedTime.String()).
			Str("busyTime", jq.busyTime.String()).
			Str("idleTime", jq.idleTime.String()).
			Msg("Job queue stats")
	}

	jq.logger.Info().Msg("Job queue stopped successfully")

	return nil
}

// pollJobs is a long-running goroutine that fetches jobs from the JobQueue DB and sends them to the worker channels.
func (jq *JobQueue[T]) pollJobs(ctx context.Context) {
	ticker := time.NewTicker(jq.fetchInterval)

	for {
		select {
		case <-ctx.Done():
			jq.logger.Debug().Msg("Context cancelled, stopped fetching jobs")
			return
		case <-ticker.C:
			jq.logger.Debug().Msg("Polling for new jobs")
			if err := jq.fetchJobs(ctx); err != nil {
				jq.logger.Error().Err(err).Msg("Error fetching jobs")
			}
		}
	}
}

func (jq *JobQueue[T]) fetchJobs(ctx context.Context) error { //nolint:gocognit
	jobs, err := jq.db.FetchJobs(jq.jobsPerFetch)
	if err != nil {
		return fmt.Errorf("failed to fetch jobs: %w", err)
	}
	for _, job := range jobs {
		if job.Status == JobStatusPending {
			// If the job is already fetched, skip it
			_, ok := jq.isJobIDInQueue.Load(job.ID)
			if ok {
				continue
			}
		}
		select {
		case <-ctx.Done():
			jq.logger.Debug().Msg("Context cancelled, stopping iteration")
			return nil // stop the fetch loop, but don't return an error

		case jq.jobs <- job:
			jq.isJobIDInQueue.Store(job.ID, true)
			jq.logger.Debug().Uint64("jobID", job.ID).Msg("New job found and sent to worker")

		default:
			jq.logger.Warn().Uint64("JobID", job.ID).Msg("Found jobs, but job channel is full")
			return errJobChannelFull
		}
	}
	return nil
}
