package jobqueue

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v4"
	"github.com/goccy/go-json"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

const defaultJobIDSequenceSize = 100
const jobDBKeyPrefix = "job-"
const jobPrefetchSize = 10

type JobQueueDbBadger[T any] struct {
	db         *badger.DB
	dbPath     string
	dbInMemory bool
	jobID      *badger.Sequence
	logger     zerolog.Logger
}

func NewJobQueueDbBadger[T any](inMemory bool) JobQueueDb[T] {
	return &JobQueueDbBadger[T]{
		db:         nil,
		dbPath:     "",
		dbInMemory: inMemory,
		jobID:      nil,
		logger:     log.With().Str("module", "JobQueue").Str("dbType", "Badger").Logger(),
	}
}

func (jqdb *JobQueueDbBadger[T]) Open(path string, queueName string) error {
	jqdb.dbPath = path

	var opts badger.Options
	if jqdb.dbInMemory {
		opts = badger.DefaultOptions("").WithInMemory(true)
	} else {
		opts = badger.DefaultOptions(jqdb.dbPath)
	}
	opts.Logger = nil

	// open the BadgerDB
	db, err := badger.Open(opts)
	if err != nil {
		return fmt.Errorf("failed to open BadgerDB: %w", err)
	}
	jqdb.db = db

	// setup the job ID sequence
	jobID, err := jqdb.db.GetSequence([]byte("nextJobID"), defaultJobIDSequenceSize)
	if err != nil {
		return fmt.Errorf("failed to get next job ID sequence: %w", err)
	} else {
		jqdb.jobID = jobID
	}
	return err
}

func (jqdb *JobQueueDbBadger[T]) Close() error {
	//jqdb.logger.Debug().Msg("Closing Badger DB connection")
	if err := jqdb.jobID.Release(); err != nil {
		jqdb.logger.Error().Err(err).Msg("Failed to release next job id sequence")
	}
	if err := jqdb.db.Close(); err != nil {
		jqdb.logger.Error().Err(err).Msg("Failed to close Badger DB connection")
		return err
	}
	return nil
}

func (jqdb *JobQueueDbBadger[T]) GetNextJobId() (uint64, error) {
	id, err := jqdb.jobID.Next()
	return id, err
}

func (jqdb *JobQueueDbBadger[T]) FetchJobs(count int) ([]*job[T], error) {
	// create a new array of jobs
	jobs := make([]*job[T], 0, count)

	err := jqdb.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = jobPrefetchSize
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek([]byte(jobDBKeyPrefix)); it.ValidForPrefix([]byte(jobDBKeyPrefix)); it.Next() {
			item := it.Item()
			err := item.Value(func(v []byte) error {
				var job job[T]
				if err := json.Unmarshal(v, &job); err != nil {
					jqdb.logger.Error().Err(err).Uint64("jobID",
						binary.BigEndian.Uint64(item.Key())).Msg("Failed to unmarshal job")
					return err
				}
				jobs = append(jobs, &job)
				return nil
			})
			if err != nil {
				jqdb.logger.Error().Err(err).Uint64("jobID",
					binary.BigEndian.Uint64(item.Key())).Msg("Failed fetch job")
				return err
			}
		}
		return nil
	})
	if err != nil && len(jobs) == 0 {
		// only return an error if we didn't fetch any jobs at all. If we fetched some jobs, we can still process them.
		return nil, fmt.Errorf("failed to fetch any jobs: %w", err)
	}

	return jobs, nil
}

func (jqdb *JobQueueDbBadger[T]) ReadJob(jobID uint64) (*job[T], error) {
	var val []byte
	var theItem *badger.Item
	err := jqdb.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(dbKeyForJob(jobID)))
		if err != nil {
			return err
		}
		theItem = item
		val, err = item.ValueCopy(nil)
		return err
	})
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil, ErrJobNotFound
		}
		return nil, fmt.Errorf("failed to read job: %w", err)
	}
	var theJob job[T]
	if err := json.Unmarshal(val, &theJob); err != nil {
		jqdb.logger.Error().Err(err).Uint64("jobID",
			binary.BigEndian.Uint64(theItem.Key())).Msg("Failed to unmarshal job")
		return nil, err
	}

	return &theJob, nil
}

func (jqdb *JobQueueDbBadger[T]) AddJob(job *job[T]) (uint64, error) {
	id, err := jqdb.GetNextJobId()
	if err != nil {
		return 0, fmt.Errorf("failed to get next job id: %w", err)
	}
	job.ID = id
	jobBytes, err := json.Marshal(job)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal job: %w", err)
	}

	err = jqdb.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(dbKeyForJob(job.ID), jobBytes); err != nil {
			return fmt.Errorf("failed to store job: %w", err)
		}
		return nil
	})
	return job.ID, err
}

func (jqdb *JobQueueDbBadger[T]) DeleteJob(jobID uint64) error {
	err := jqdb.db.Update(func(txn *badger.Txn) error {
		if err := txn.Delete(dbKeyForJob(jobID)); err != nil {
			return err
		}
		return nil
	})
	return err
}

// dbKey BadgerDB iterates over keys in lexicographical order, so we need to make sure that the job ID
// is strictly increasing to avoid queues being processed out of order.
// FIXME: not sure this is relevant anymore, since we can execute jobs in parallel
func dbKeyForJob(jobId uint64) []byte {
	return []byte(fmt.Sprintf("%s%d", jobDBKeyPrefix, jobId))
}
