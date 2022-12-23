package loghttp

import (
	"errors"
	"sync"
)

type Record struct {
	Offset uint64 `json:"offset"`
	Value  []byte `json:"value"`
}

type Log struct {
	sync.RWMutex
	records []Record
}

var ErrOffsetNotFound = errors.New("offset not found")

func NewLogger() *Log {
	return &Log{}
}

func (c *Log) Append(record Record) (uint64, error) {
	c.Lock()
	defer c.Unlock()

	record.Offset = uint64(len(c.records))
	c.records = append(c.records, record)
	return record.Offset, nil
}

func (c *Log) Read(offset uint64) (Record, error) {
	c.RLock()
	defer c.RUnlock()

	if offset >= uint64(len(c.records)) {
		return Record{}, ErrOffsetNotFound
	}
	return c.records[offset], nil
}
