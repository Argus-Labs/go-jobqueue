package jobqueue

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// JobQueueDbMongo is the MongoDB implementation of the JobQueueDb interface
type JobQueueDbMongo[T any] struct {
	client       *mongo.Client
	ctx          context.Context
	db           *mongo.Database
	coll         *mongo.Collection
	idColl       *mongo.Collection
	jobQueueName string
}

// NewJobQueueDbMongo creates a new JobQueueDbMongo instance
func NewJobQueueDbMongo[T any](ctx context.Context) JobQueueDb[T] {
	return &JobQueueDbMongo[T]{
		client:       nil,
		ctx:          ctx,
		db:           nil,
		coll:         nil,
		jobQueueName: "",
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
	// holds the jobs for the queue
	jqdb.jobQueueName = dbCollectionNameForQueue(queueName)
	jqdb.coll = jqdb.db.Collection(jqdb.jobQueueName)
	if jqdb.coll == nil {
		return fmt.Errorf("failed to open collection job_queues.%s", jqdb.jobQueueName)
	}
	// holds the job IDs for all queues
	jqdb.idColl = jqdb.db.Collection("job_ids")
	if jqdb.idColl == nil {
		return fmt.Errorf("failed to open collection job_queues.job_ids")
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

	var nextJobId uint64
	result := jqdb.idColl.FindOneAndUpdate(jqdb.ctx,
		bson.D{{Key: "queue", Value: jqdb.jobQueueName}},                     // selector
		bson.D{{Key: "$inc", Value: bson.D{{Key: "next_job_id", Value: 1}}}}) // update, increment next_job_id by 1, return old record
	if result.Err() != nil {
		if errors.Is(result.Err(), mongo.ErrNoDocuments) {
			// insert the queue if it doesn't exist. We start with ID 2 because we are returning 1
			_, err := jqdb.idColl.InsertOne(jqdb.ctx, bson.D{{Key: "queue", Value: jqdb.jobQueueName}, {Key: "next_job_id", Value: 2}})
			if err != nil {
				return 0, fmt.Errorf("failed to create initial mongo record for next job id: %w", err)
			}
			nextJobId = 1
		} else {
			return 0, fmt.Errorf("failed to get next job id: %w", result.Err())
		}
	}
	raw, err := result.Raw()
	if err != nil {
		return 0, fmt.Errorf("failed to get raw result from mongo: %w", err)
	}
	val := raw.Lookup("next_job_id")
	nextJobId = uint64(val.AsInt64())
	return nextJobId, nil
}

// FetchJobs(count int) ([]*job[T], error)
func (jqdb *JobQueueDbMongo[T]) FetchJobs(count int) ([]*job[T], error) {
	return nil, nil
}

// ReadJob(jobID uint64) (*job[T], error)
func (jqdb *JobQueueDbMongo[T]) ReadJob(jobID uint64) (*job[T], error) {
	result := jqdb.coll.FindOne(jqdb.ctx, bson.D{{Key: "id", Value: jobID}})
	if result.Err() != nil {
		if errors.Is(result.Err(), mongo.ErrNoDocuments) {
			return nil, ErrJobNotFound
		}
		return nil, fmt.Errorf("failed to read job from mongo collection: %w", result.Err())
	}
	var j job[T]
	err := result.Decode(&j)
	if err != nil {
		return nil, fmt.Errorf("failed to decode job from mongo collection: %w", err)
	}
	return &j, nil
}

// AddJob(job *job[T]) (uint64, error) // returns the job ID
func (jqdb *JobQueueDbMongo[T]) AddJob(job *job[T]) (uint64, error) {
	id, err := jqdb.GetNextJobId()
	if err != nil {
		return 0, fmt.Errorf("failed to get next job id: %w", err)
	}
	job.ID = id
	_, err = jqdb.coll.InsertOne(jqdb.ctx, job)
	if err != nil {
		return 0, fmt.Errorf("failed to insert job into mongo collection: %w", err)
	}
	return job.ID, nil
}

// DeleteJob(jobID uint64) error
func (jqdb *JobQueueDbMongo[T]) DeleteJob(jobID uint64) error {
	result := jqdb.coll.FindOneAndDelete(jqdb.ctx, bson.D{{Key: "id", Value: jobID}})
	if !errors.Is(result.Err(), mongo.ErrNoDocuments) {
		return fmt.Errorf("failed to delete job from mongo collection: %w", result.Err())
	}
	return nil
}

func dbCollectionNameForQueue(queueName string) string {
	// TODO: normalize queueName
	return queueName + "_jobs"
}
