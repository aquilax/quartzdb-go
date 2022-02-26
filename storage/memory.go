package storage

import (
	"fmt"
	"time"
)

const day = time.Hour * 24

type MemoryShard struct {
	date    time.Time
	records []*RawRecord
}

type Memory struct {
	mode    StorageMode
	storage map[string]*MemoryShard
}

func NewMemory(mode StorageMode) *Memory {
	return &Memory{
		mode:    mode,
		storage: make(map[string]*MemoryShard),
	}
}

func (s Memory) GetMode() StorageMode {
	return s.mode
}

func (s *MemoryShard) Add(records []Record) (int, error) {
	added := 0
	for _, r := range records {
		if !s.date.Equal(r.Time().Truncate(day)) {
			return -1, fmt.Errorf("attempt to add record to wrong shard")
		}
		s.records = append(s.records, &RawRecord{r.Time(), r.Bytes()})
	}
	return added, nil
}

func (s MemoryShard) GetDate() time.Time {
	return s.date
}

func (s MemoryShard) GetAllCallback(callback QueryCallback) error {
	for _, r := range s.records {
		stop, err := callback(r)
		if stop || err != nil {
			return err
		}
	}
	return nil
}

func (s *Memory) GetShard(ts time.Time) (Shard, error) {
	key := getKey(ts)
	_, exists := s.storage[key]
	if !exists {
		(*s).storage[key] = &MemoryShard{ts.Truncate(day), make([]*RawRecord, 0)}
	}
	return s.storage[key], nil
}

func (s *Memory) GetShards() ([]Shard, error) {
	shards := make([]Shard, len(s.storage))
	i := 0
	for k := range s.storage {
		shards[i] = s.storage[k]
		i++
	}
	return shards, nil
}

func (s *Memory) GetShardsRange(from time.Time, to time.Time) ([]Shard, error) {
	shards := make([]Shard, 0)
	fromKey := getKey(from)
	toKey := getKey(to)
	for d := range s.storage {
		if d >= fromKey && d <= toKey {
			shards = append(shards, s.storage[d])
		}
	}
	return shards, nil
}

func getKey(date time.Time) string {
	return date.Format("2006-02-01")
}
