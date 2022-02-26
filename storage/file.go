package storage

import (
	"os"
	"time"
)

type File struct {
	path string
	mode StorageMode
	flag int
	perm os.FileMode
}

func NewFile(path string, mode StorageMode, perm os.FileMode) *File {
	flag := os.O_RDONLY
	if mode == ModeWrite {
		flag = os.O_APPEND | os.O_CREATE | os.O_WRONLY
	}
	return &File{
		path: path,
		mode: mode,
		flag: flag,
		perm: perm,
	}
}

func (s File) GetMode() StorageMode {
	return s.mode
}

func (s File) Add(date time.Time, record Record) (int, error) {
	return -1, nil
}
