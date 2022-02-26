package quartzdb

import (
	"reflect"
	"testing"
	"time"

	"github.com/aquilax/quartzdb-go/storage"
)

func assert(t *testing.T, condition bool, message string) {
	if !condition {
		t.Fatal(message)
	}
}

func TestQuartzDB_EndToEnd(t *testing.T) {
	memStorage := storage.NewMemory(storage.ModeWrite)
	db := NewQuartzDB(memStorage)

	ts := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

	rec1 := storage.NewRawRecord(ts, []byte(`{"test": 1}`))
	rec2 := storage.NewRawRecord(ts.Add(24*time.Hour), []byte(`{"test": 2}`))

	rec3 := storage.NewRawRecord(ts.Add(48*time.Hour), []byte(`{"test": 3}`))
	rec4 := storage.NewRawRecord(ts.Add(48*time.Hour), []byte(`{"test": 4}`))
	rec5 := storage.NewRawRecord(ts.Add(48*time.Hour), []byte(`{"test": 5}`))

	if _, err := db.Add(rec1); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Add(rec2); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Add(rec3); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Add(rec4); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Add(rec5); err != nil {
		t.Fatal(err)
	}

	// test GetByDate
	r, _ := db.GetByDate(ts)
	assert(t, r != nil, "record is found")
	if r != nil {
		assert(t, reflect.DeepEqual((*r).Bytes(), rec1.Bytes()), "expected rec1")
	}
	if r == nil {
		t.Fatalf("expected record but nil returned")
	}

	// test QueryLast
	res, _ := db.QueryLast(2)
	assert(t, len(res) == 2, "expected two records in the last shard")
	assert(t, reflect.DeepEqual(res[0].Bytes(), rec4.Bytes()), "expected rec4 first")
	assert(t, reflect.DeepEqual(res[1].Bytes(), rec5.Bytes()), "expected rec5 last")
}
