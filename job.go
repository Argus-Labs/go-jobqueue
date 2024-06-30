package jobqueue

import (
	"fmt"
	"time"
)

const jobDBKeyPrefix = "job-"

type JobHandler = func(JobContext, struct{}) error

// JobContext provides context for a job which is injected into the job Process method.
type JobContext interface {
	JobID() uint64
	JobCreatedAt() time.Time
}

// Type job must implement the JobContext interface
var _ JobContext = (*job[struct{}])(nil)

// job is an internal representation of a job in the job queue.
type job[T any] struct {
	ID        uint64    `json:"id"`
	Payload   T         `json:"payload"`
	Status    JobStatus `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

func newJob[T any](id uint64, payload T) *job[T] {
	return &job[T]{
		ID:        id,
		Payload:   payload,
		Status:    JobStatusPending,
		CreatedAt: time.Now(),
	}
}

func (j *job[T]) JobID() uint64 {
	return j.ID
}

func (j *job[T]) JobCreatedAt() time.Time {
	return j.CreatedAt
}

func (j *job[T]) Process(handler func(JobContext, T) error) error {
	// Attempt to process the job
	if err := handler(j, j.Payload); err != nil {
		return err
	}

	// If the job was processed successfully, update the status
	j.Status = JobStatusCompleted

	return nil
}

// dbKey BadgerDB iterates over keys in lexicographical order, so we need to make sure that the job ID
// is strictly increasing to avoid queues being processed out of order.
func (j *job[T]) dbKey() []byte {
	return []byte(fmt.Sprintf("%s%d", jobDBKeyPrefix, j.ID))
}
