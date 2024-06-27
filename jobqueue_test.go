package jobqueue

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
}

type TestJob struct {
	Msg string
}

func (j TestJob) Process(ctx JobContext) error {
	fmt.Println("Job processed:", j.Msg, ctx.JobID(), ctx.JobCreatedAt().Unix())
	return nil
}

func TestJobQueue(t *testing.T) {
	// Open the Badger database
	opts := badger.DefaultOptions("/tmp/badger")
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	jq, err := NewJobQueue[TestJob](db, "test-job", 2)
	assert.NoError(t, err)

	t.Run("Enqueue", func(t *testing.T) {
		j := TestJob{Msg: "hello"}

		id, err := jq.Enqueue(j)
		assert.NoError(t, err)

		// Verify job was stored in mock DB
		value, err := readKey(db, id)
		assert.NoError(t, err)

		var recoveredJobEntry job[TestJob]
		err = json.Unmarshal(value, &recoveredJobEntry)
		assert.NoError(t, err)

		assert.Equal(t, id, recoveredJobEntry.ID)
		assert.Equal(t, j, recoveredJobEntry.Payload)
		assert.Equal(t, JobStatusPending, recoveredJobEntry.Status)
		assert.WithinDuration(t, time.Now(), recoveredJobEntry.CreatedAt, time.Second)
	})

	t.Run("ProcessJob", func(t *testing.T) {
		j := &job[TestJob]{
			ID:        uuid.NewString(),
			Payload:   TestJob{Msg: "world"},
			Status:    JobStatusPending,
			CreatedAt: time.Now(),
		}

		err = jq.processJob(j)
		require.NoError(t, err)

		// Verify job status was updated in mock DB
		value, err := readKey(db, j.ID)
		require.NoError(t, err)

		var recoveredJobEntry job[TestJob]
		err = json.Unmarshal(value, &recoveredJobEntry)
		require.NoError(t, err)

		assert.Equal(t, JobStatusCompleted, recoveredJobEntry.Status)
	})
}

func readKey(db *badger.DB, key string) ([]byte, error) {
	var valCopy []byte
	err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}

		valCopy, err = item.ValueCopy(nil)
		return err
	})

	if err != nil {
		return nil, err
	}

	return valCopy, nil
}
