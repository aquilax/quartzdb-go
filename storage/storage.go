package storage

import "time"

// Mode determines the client data access mode read/read-write
type Mode rune

const (
	// ModeRead opens the database in read only mode
	ModeRead Mode = 'r'
	// ModeWrite opens the database in read-write mode
	ModeWrite Mode = 'w'
)

var truncateDay = time.Hour * 24

// Record is an interface for single record item. Every record must be timestamped and serializeabe to array of bytes
type Record interface {
	Time() time.Time
	Bytes() []byte
}

// QueryCallback is a callback function function used to iterate over records
type QueryCallback = func(record Record) (stop bool, err error)

// Shard is a date based shard of records. A shard must contain only records from the same UTC date
type Shard interface {
	GetDate() time.Time
	Add(records []Record) (int, error)
	GetAllCallback(callback QueryCallback) error
}

// QuartzDBStorage is a QuartzDB storage interface
type QuartzDBStorage interface {
	GetMode() Mode
	GetShard(ts time.Time) (Shard, error)
	GetShards() ([]Shard, error)
	GetShardsRange(from time.Time, to time.Time) ([]Shard, error)
}
