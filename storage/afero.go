package storage

import (
	"bufio"
	"fmt"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/spf13/afero"
)

type AferoShard struct {
	fs afero.Fs
	FileShard
}

type Afero struct {
	fs   afero.Fs
	path string
	mode Mode
	flag int
	perm os.FileMode
}

func NewAfero(fs afero.Fs, path string, mode Mode, perm os.FileMode) *Afero {
	flag := os.O_RDONLY
	if mode == ModeWrite {
		flag = os.O_APPEND | os.O_CREATE | os.O_WRONLY
	}
	return &Afero{
		fs:   fs,
		path: path,
		mode: mode,
		flag: flag,
		perm: perm,
	}
}

func (s Afero) GetMode() Mode {
	return s.mode
}

func (s Afero) GetShard(ts time.Time) (Shard, error) {
	date := ts.UTC().Truncate(truncateDay)

	d := getDirectory(ts)
	dirFullPath := path.Join(s.path, d)

	var err error
	if _, err = s.fs.Stat(dirFullPath); os.IsNotExist(err) {
		if s.mode == ModeRead {
			return nil, err
		}
		if s.mode == ModeWrite {
			err = s.fs.MkdirAll(dirFullPath, os.ModePerm)
			if err != nil {
				return nil, err
			}
		}
	}

	fileName := fmt.Sprintf("%02d.txt", ts.Day())
	fileFullPath := path.Join(dirFullPath, fileName)

	if _, err = s.fs.Stat(fileFullPath); os.IsNotExist(err) {
		if s.mode == ModeRead {
			return nil, err
		}
		if s.mode == ModeWrite {
			file, err := s.fs.OpenFile(fileFullPath, s.flag, s.perm)
			if err != nil {
				return nil, err
			}
			file.Close()
		}
	}

	return &AferoShard{s.fs, FileShard{date: date, path: fileFullPath, flag: s.flag, perm: s.perm}}, nil
}

func (s Afero) GetShards() ([]Shard, error) {
	shards := make([]Shard, 0)
	yearEntries, err := afero.ReadDir(s.fs, s.path)
	if err != nil {
		return shards, err
	}
	for _, y := range yearEntries {
		if y.IsDir() && len(y.Name()) == 4 {
			year, err := strconv.Atoi(y.Name())
			if err != nil {
				return shards, err
			}
			monthEntries, err := afero.ReadDir(s.fs, path.Join(s.path, y.Name()))
			if err != nil {
				return shards, err
			}
			for _, m := range monthEntries {
				if m.IsDir() && len(m.Name()) == 2 {
					month, err := strconv.Atoi(m.Name())
					if err != nil {
						return shards, err
					}
					dayEntries, err := afero.ReadDir(s.fs, path.Join(s.path, y.Name(), m.Name()))
					if err != nil {
						return shards, err
					}
					for _, d := range dayEntries {
						if !d.IsDir() && len(d.Name()) == 6 {
							day, err := strconv.Atoi(d.Name()[:2])
							if err != nil {
								return shards, err
							}
							date := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
							path := path.Join(s.path, y.Name(), m.Name(), d.Name())
							shard := &AferoShard{s.fs, FileShard{date: date, path: path, flag: s.flag, perm: s.perm}}
							shards = append(shards, shard)
						}
					}
				}
			}
		}
	}

	return shards, nil
}

func (s Afero) GetShardsRange(from time.Time, to time.Time) ([]Shard, error) {
	shards := make([]Shard, 0)
	target := to.UTC().Truncate(truncateDay)
	current := from.UTC().Truncate(truncateDay)
	for {
		shard, err := s.GetShard(current)
		if err != nil {
			return shards, err
		}
		shards = append(shards, shard)
		if current.Equal(target) {
			break
		}
		current = current.AddDate(0, 0, 1)
	}
	return shards, nil
}

func (s AferoShard) GetDate() time.Time {
	return s.date
}

func (s AferoShard) Add(records []Record) (int, error) {
	added := 0
	file, err := s.fs.OpenFile(s.path, s.flag, s.perm)
	if err != nil {
		return -1, nil
	}
	defer file.Close()
	for _, r := range records {
		if !s.date.Equal(r.Time().Truncate(truncateDay)) {
			return -1, fmt.Errorf("attempt to add record to wrong shard")
		}
		_, err = fmt.Fprintf(file, "%s %s\n", getTimestamp(r.Time()), r.Bytes())
		if err != nil {
			return added, err
		}
		added += 1
	}
	return added, nil

}

func (s AferoShard) GetAllCallback(callback QueryCallback) error {
	file, err := s.fs.Open(s.path)
	if err != nil {
		return nil
	}
	scanner := bufio.NewScanner(file)
	line := 0
	for scanner.Scan() {
		bytes := scanner.Bytes()
		if len(bytes) < 27 {
			return fmt.Errorf("invalid line #%d in %s", line+1, s.path)
		}
		tsString := string(bytes[:26])
		ts, err := time.Parse(timestampLayout, tsString)
		if err != nil {
			return err
		}
		stop, err := callback(RawRecord{ts, bytes[27:]})
		if stop || err != nil {
			return err
		}
		line++
	}
	return scanner.Err()
}
