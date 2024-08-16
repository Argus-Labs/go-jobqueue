package jobqueue

import (
	"context"
	"fmt"

	//	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// JobQueueDbMongo is the MongoDB implementation of the JobQueueDb interface
type JobQueueDbMongo[T any] struct {
	client *mongo.Client
	ctx    context.Context
	db     *mongo.Database
	coll   *mongo.Collection
}

// NewJobQueueDbMongo creates a new JobQueueDbMongo instance
func NewJobQueueDbMongo[T any]() JobQueueDb[T] {
	return &JobQueueDbMongo[T]{
		client: nil,
		ctx:    context.TODO(),
		db:     nil,
		coll:   nil,
	}
}

// Open the MongoDB database
func (jqdb *JobQueueDbMongo[T]) Open(path string, queueName string) error {
	client, err := mongo.Connect(jqdb.ctx, options.Client().ApplyURI(path))
	if err != nil {
		return fmt.Errorf("failed to connect to MongoDB at %s: %w", path, err)
	}
	jqdb.client = client
	// TODO: handle mongo db options
	jqdb.db = client.Database("job_queues")
	if jqdb.db == nil {
		return fmt.Errorf("failed to open mongo database job_queues")
	}
	jqdb.coll = jqdb.db.Collection(queueName + "_jobs")
	if jqdb.coll == nil {
		return fmt.Errorf("failed to open collection job_queues.%s_jobs", queueName)
	}
	return nil
}

// Close the MongoDB database
func (jqdb *JobQueueDbMongo[T]) Close() error {
	err := jqdb.client.Disconnect(jqdb.ctx)
	return err
}

// GetNextJobId() (uint64, error)
func (jqdb *JobQueueDbMongo[T]) GetNextJobId() (uint64, error) {
	return 0, nil
}

// FetchJobs(count int) ([]*job[T], error)
func (jqdb *JobQueueDbMongo[T]) FetchJobs(count int) ([]*job[T], error) {
	return nil, nil
}

// ReadJob(jobID uint64) (*job[T], error)
func (jqdb *JobQueueDbMongo[T]) ReadJob(jobID uint64) (*job[T], error) {
	return nil, nil
}

// UpdateJob(job *job[T]) error
func (jqdb *JobQueueDbMongo[T]) UpdateJob(job *job[T]) error {
	return nil
}

// AddJob(job *job[T]) (uint64, error) // returns the job ID
func (jqdb *JobQueueDbMongo[T]) AddJob(job *job[T]) (uint64, error) {
	return 0, nil
}

// DeleteJob(jobID uint64) error
func (jqdb *JobQueueDbMongo[T]) DeleteJob(jobID uint64) error {
	return nil
}
