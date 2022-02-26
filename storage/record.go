package storage

import "time"

// RawRecord implements Record which can contain any raw bytes data
type RawRecord struct {
	ts    time.Time
	bytes []byte
}

// NewRawRecord creates new RawRecord
func NewRawRecord(ts time.Time, bytes []byte) RawRecord {
	return RawRecord{ts, bytes}
}

// Time returns the record time
func (mr RawRecord) Time() time.Time {
	return mr.ts
}

// Bytes returns the record content
func (mr RawRecord) Bytes() []byte {
	return mr.bytes
}
