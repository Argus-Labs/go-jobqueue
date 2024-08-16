package jobqueue

type JobQueueDb[T any] interface {
	Open(path string, queueName string) error
	Close() error
	GetNextJobId() (uint64, error)
	FetchJobs(count int) ([]*job[T], error)
	ReadJob(jobID uint64) (*job[T], error)
	UpdateJob(job *job[T]) error
	AddJob(job *job[T]) (uint64, error) // returns the job ID
	DeleteJob(jobID uint64) error
}
