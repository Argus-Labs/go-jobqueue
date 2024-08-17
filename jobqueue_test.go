package jobqueue

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const BadgerDBPath = "/tmp/badger"

func init() { //nolint:gochecknoinits // for testing
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
}

type testJob struct {
	Msg string
}

func testJobHandler() func(JobContext, testJob) error {
	return func(ctx JobContext, job testJob) error {
		fmt.Println("Job Performed:", job.Msg, ctx.JobID(), //nolint:forbidigo // for testing
			ctx.JobCreatedAt().Unix())
		return nil
	}
}

func complexJobHandler() func(JobContext, testJob) error {
	return func(ctx JobContext, job testJob) error {
		fmt.Println("Starting job...", job.Msg, ctx.JobID(), //nolint:forbidigo // for testing
			ctx.JobCreatedAt().Unix())
		numMicroseconds := rand.Int63n(200) + 1
		time.Sleep(time.Duration(numMicroseconds) * time.Microsecond)
		fmt.Println("Job completed:", job.Msg, ctx.JobID(), //nolint:forbidigo // for testing
			ctx.JobCreatedAt().Unix(), "after "+strconv.FormatInt(numMicroseconds, 10)+"µs")
		return nil
	}
}

func TestNewJobQueue(t *testing.T) {
	t.Parallel()

	// Test cases
	testCases := []struct {
		name          string
		dbPath        string
		queueName     string
		workers       int
		options       []Option[testJob]
		expectedError bool
		cleanupNeeded bool
	}{
		{
			name:          "Valid configuration",
			dbPath:        "/tmp/test_jobqueue_1",
			queueName:     "test-queue-1",
			workers:       2,
			options:       []Option[testJob]{WithInMemDB[testJob]()},
			expectedError: false,
		},
		{
			name:          "Invalid workers count",
			dbPath:        "/tmp/test_jobqueue_2",
			queueName:     "test-queue-2",
			workers:       -1,
			options:       []Option[testJob]{WithInMemDB[testJob]()},
			expectedError: true,
		},
		{
			name:          "Zero workers",
			dbPath:        "/tmp/test_jobqueue_3",
			queueName:     "test-queue-3",
			workers:       0,
			options:       []Option[testJob]{WithInMemDB[testJob]()},
			expectedError: false,
		},
	}

	for _, tc := range testCases {
		tc := tc // capture range variable
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Act
			jq, err := New[testJob](tc.dbPath, tc.queueName, tc.workers, testJobHandler(), tc.options...)

			// Assert
			if tc.expectedError {
				assert.Error(t, err)
				assert.Nil(t, jq)
			} else {
				require.NoError(t, err)
				require.NotNil(t, jq)

				assert.NotNil(t, jq.db)
				assert.NotNil(t, jq.isJobIDInQueue)
				assert.NotNil(t, jq.jobs)

				// Cleanup
				err = jq.Stop()
				assert.NoError(t, err)
			}
		})
	}
}

func TestJobQueue_Enqueue(t *testing.T) {
	cleanupBadgerDB(t)

	jq, err := New[testJob](BadgerDBPath, "test-job", 0, testJobHandler(), WithInMemDB[testJob]())
	assert.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, jq.Stop())
	})

	for i := 0; i < 10; i++ {
		j := testJob{Msg: fmt.Sprintf("hello %d", i)}

		id, err := jq.Enqueue(j)
		assert.NoError(t, err)

		// Verify that the job was stored in badger DB
		dbJob, err := jq.db.ReadJob(id)
		assert.NoError(t, err)

		// Verify that the job is what we're expecting
		assert.Equal(t, id, dbJob.ID)
		assert.Equal(t, j, dbJob.Payload)
		assert.Equal(t, JobStatusPending, dbJob.Status)
		assert.WithinDuration(t, time.Now(), dbJob.CreatedAt, time.Second)
	}
}

func TestJobQueue_ProcessJob(t *testing.T) {
	cleanupBadgerDB(t)

	jq, err := New[testJob](BadgerDBPath, "test-job", 0, testJobHandler(), WithInMemDB[testJob]())
	assert.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, jq.Stop())
	})

	// Queue a bunch of jobs
	ids := make([]uint64, 0)
	for i := 0; i < 10; i++ {
		j := testJob{Msg: fmt.Sprintf("hello %d", i)}

		id, err := jq.Enqueue(j)
		assert.NoError(t, err)

		ids = append(ids, id)
	}

	// Blocks until the job is fetched from badger
	for i := 0; i < 10; i++ {
		j := <-jq.jobs

		// Check that the job is what we're expecting
		assert.Equal(t, ids[i], j.ID)
		assert.Equal(t, testJob{Msg: fmt.Sprintf("hello %d", i)}, j.Payload)
		assert.Equal(t, JobStatusPending, j.Status)
		assert.WithinDuration(t, time.Now(), j.CreatedAt, time.Second)

		// Process the job
		assert.NoError(t, jq.processJob(j, 0))

		// Check that the job is removed from the in-memory index
		_, ok := jq.isJobIDInQueue.Load(ids[i])
		assert.False(t, ok)

		// Check that the job is no longer in the badger DB
		dbJob, err := jq.db.ReadJob(ids[i])
		assert.Error(t, err, badger.ErrKeyNotFound)
		assert.Nil(t, dbJob)
	}
}

func TestJobQueue_Recovery(t *testing.T) {
	cleanupBadgerDB(t)

	// Create initial job queue
	jq, err := New[testJob]("/tmp/badger", "test-job", 0, testJobHandler())
	assert.NoError(t, err)

	t.Cleanup(func() {
		cleanupBadgerDB(t)
	})

	// Enqueue job to initial job queue
	id, err := jq.Enqueue(testJob{Msg: "hello"})
	assert.NoError(t, err)

	// Stop initial job queue
	assert.NoError(t, jq.Stop())

	// Create recovered job queue
	recoveredJq, err := New[testJob]("/tmp/badger", "test-job", 0, testJobHandler())
	assert.NoError(t, err)

	j := <-recoveredJq.jobs

	// Verify that the job is recovered correctly
	assert.Equal(t, id, j.ID)
	assert.Equal(t, j.Payload, testJob{Msg: "hello"})

	// Process the job in recovered job queue
	assert.NoError(t, recoveredJq.processJob(j, 0))

	// Stop recovered job queue
	assert.NoError(t, recoveredJq.Stop())
}

func TestJobConcurrency(t *testing.T) {
	cleanupBadgerDB(t)

	// create initial job queue

	// mongo version
	jq, err := New[testJob]("ignored", "test-job", 5, complexJobHandler(),
		UseMongoDB[testJob]("mongodb://localhost:27017"))

	// badger version
	//jq, err := New[testJob]("/tmp/badger", "test-job", 5, complexJobHandler())

	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, jq.Stop())
		cleanupBadgerDB(t)
	})

	// Queue a bunch of jobs, which should be processed concurrently
	ids := make([]uint64, 0)
	for i := 0; i < 20; i++ {
		j := testJob{Msg: fmt.Sprintf("hello %d", i)}

		id, err := jq.Enqueue(j)
		assert.NoError(t, err)

		ids = append(ids, id)
	}

	// Give time for all jobs to be processed
	time.Sleep(time.Second)

	// add a few more new jobs
	for i := 20; i < 22; i++ {
		j := testJob{Msg: fmt.Sprintf("hello %d", i)}

		id, err := jq.Enqueue(j)
		assert.NoError(t, err)

		ids = append(ids, id)
	}

	// more time for new jobs to be processed
	time.Sleep(time.Second)

	// Check that all jobs were processed
	for id := range ids {
		// Check that the job is removed from the in-memory index
		_, ok := jq.isJobIDInQueue.Load(uint64(id))
		assert.False(t, ok)

		// Check that the job is no longer in the badger DB
		job, err := jq.db.ReadJob(uint64(id))
		assert.Error(t, err, badger.ErrKeyNotFound)
		assert.Nil(t, job)
	}

}

func cleanupBadgerDB(t *testing.T) {
	assert.NoError(t, os.RemoveAll(BadgerDBPath))
}
