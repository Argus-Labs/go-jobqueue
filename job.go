package jobqueue

import (
	"time"

	"github.com/google/uuid"
)

// JobType is a user-defined job struct that is processed by the job queue.
// The struct must implement the Process method.
type JobType interface {
	Process(JobContext) error
}

// JobContext provides context for a job which is injected into the job Process method.
type JobContext interface {
	JobID() string
	JobCreatedAt() time.Time
}

// Type job must implement the JobContext interface
var _ JobContext = (*job[JobType])(nil)

// job is an internal representation of a job in the job queue.
type job[T JobType] struct {
	ID        string    `json:"id"`
	Payload   T         `json:"payload"`
	Status    JobStatus `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

func newJob[T JobType](payload T) *job[T] {
	return &job[T]{
		ID:        uuid.NewString(), // Implement this function to generate unique IDs
		Payload:   payload,
		Status:    JobStatusPending,
		CreatedAt: time.Now(),
	}
}

func (j *job[T]) JobID() string {
	return j.ID
}

func (j *job[T]) JobCreatedAt() time.Time {
	return j.CreatedAt
}

func (j *job[T]) Process() error {
	// Attempt to process the job
	if err := j.Payload.Process(j); err != nil {
		return err
	}

	// If the job was processed successfully, update the status
	j.Status = JobStatusCompleted

	return nil
}
