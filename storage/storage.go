package storage

import "time"

type StorageMode rune

const (
	ModeRead  StorageMode = 'r'
	ModeWrite StorageMode = 'w'
)

type Record interface {
	Time() time.Time
	Bytes() []byte
}

type QueryCallback = func(record Record) (stop bool, err error)

type Shard interface {
	GetDate() time.Time
	Add(record []Record) (int, error)
	GetAllCallback(callback QueryCallback) error
}

type QuartzStorage interface {
	GetMode() StorageMode
	GetShard(ts time.Time) (Shard, error)
	GetShards() ([]Shard, error)
	GetShardsRange(from time.Time, to time.Time) ([]Shard, error)
}
