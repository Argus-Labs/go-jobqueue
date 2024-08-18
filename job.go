package jobqueue

import (
	"time"
)

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

func newJob[T any](payload T) *job[T] {
	return &job[T]{
		ID:        0, // ID is set when the job is added to the queue
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
