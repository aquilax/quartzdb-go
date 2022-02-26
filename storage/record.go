package storage

import "time"

type RawRecord struct {
	ts    time.Time
	bytes []byte
}

func NewRawRecord(ts time.Time, bytes []byte) RawRecord {
	return RawRecord{ts, bytes}
}

func (mr RawRecord) Time() time.Time {
	return mr.ts
}

func (mr RawRecord) Bytes() []byte {
	return mr.bytes
}
