// Package quartzdb implements QuartzDB client in go
// QuartzDB is flat-file database optimized to hold time-series data.
package quartzdb

import (
	"fmt"
	"time"

	"github.com/aquilax/quartzdb-go/storage"
)

// QuartzDB contains the database interface
type QuartzDB struct {
	storage storage.QuartzDBStorage
}

// NewQuartzDB creates new database instance given a storage client
func NewQuartzDB(storage storage.QuartzDBStorage) *QuartzDB {
	return &QuartzDB{storage: storage}
}

// Add appends a record to the database
func (q QuartzDB) Add(record storage.Record) (int, error) {
	if q.storage.GetMode() != storage.ModeWrite {
		return -1, fmt.Errorf("this connection is read-only")
	}
	shard, err := q.storage.GetShard(record.Time())
	if err != nil {
		return -1, err
	}
	return shard.Add([]storage.Record{record})
}

// QueryRangeCallback calls the callback function for each record that is dated between the from and to timestamps
func (q QuartzDB) QueryRangeCallback(from time.Time, to time.Time, callback storage.QueryCallback) error {
	if from.After(to) {
		return fmt.Errorf("start date is after the end date")
	}
	shards, err := q.storage.GetShardsRange(from, to)
	if err != nil {
		return err
	}
	for _, shard := range shards {
		err := shard.GetAllCallback(func(record storage.Record) (stop bool, err error) {
			if between(record.Time(), from, to) {
				callback(record)
			}
			return false, nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// QueryRange returns list of records dated between from and to timestamps
func (q QuartzDB) QueryRange(from time.Time, to time.Time) ([]storage.Record, error) {
	results := make([]storage.Record, 0)
	err := q.QueryRangeCallback(from, to, func(record storage.Record) (stop bool, err error) {
		results = append(results, record)
		return false, nil
	})
	return results, err
}

func (q QuartzDB) getLastShard() (*storage.Shard, error) {
	shards, err := q.storage.GetShards()
	if err != nil {
		return nil, err
	}
	if len(shards) < 1 {
		return nil, nil
	}
	last := shards[0]
	for i := range shards {
		if shards[i].GetDate().After(last.GetDate()) {
			last = shards[i]
		}
	}
	return &last, nil
}

// QueryLast returns the last n records from the last shard
func (q QuartzDB) QueryLast(n int) ([]storage.Record, error) {
	results := make([]storage.Record, 0)
	buffer := make([]storage.Record, n)
	if n == 0 {
		return results, nil
	}
	if n < 0 {
		return results, fmt.Errorf("limit must be e positive number")
	}
	shard, err := q.getLastShard()
	if err != nil {
		return nil, err
	}
	if shard == nil {
		return nil, nil
	}
	index := 0
	err = (*shard).GetAllCallback(func(record storage.Record) (stop bool, err error) {
		buffer[index] = record
		index = (index + 1) % n // round robin
		return false, nil
	})
	if err != nil {
		return results, nil
	}
	return append(buffer[index:], buffer[:index]...), nil
}

// GetByDate returns the first matching record by timestamp
func (q QuartzDB) GetByDate(date time.Time) (*storage.Record, error) {
	var result storage.Record
	shard, err := q.storage.GetShard(date)
	if err != nil {
		return nil, err
	}
	err = shard.GetAllCallback(func(record storage.Record) (stop bool, err error) {
		if date.Equal(record.Time()) {
			result = record
			return true, nil
		}
		return false, nil
	})
	return &result, err
}

func between(ts, from, to time.Time) bool {
	return ts.Before(from) && ts.After(to)
}
