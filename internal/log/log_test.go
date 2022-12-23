package log

import (
	"bytes"
	"io"
	"os"
	"testing"

	api "github.com/cevataykans/proglog/api/v1"
	"google.golang.org/protobuf/proto"
)

func TestLog(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, l *Log){
		"append and read a record succeeds": testAppendRead,
		"offset out of range error":         testOutOfRange,
		"testInitExisting":                  testInitExisting,
		"reader":                            testReader,
		"truncate":                          testTruncate,
	} {
		t.Run(scenario, func(t *testing.T) {
			dir, err := os.MkdirTemp("", "store-test")
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(dir)

			c := Config{}
			c.Segment.MaxStoreBytes = 32
			log, err := NewLog(dir, c)
			if err != nil {
				t.Fatal(err)
			}

			fn(t, log)
		})
	}
}

func testAppendRead(t *testing.T, l *Log) {
	append := api.Record{
		Value: []byte("hello world!"),
	}

	off, err := l.Append(&append)
	if err != nil {
		t.Fatal(err)
	}

	r, err := l.Read(off)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(r.Value, append.Value) {
		t.Fatalf("bytes are not equal")
	}
}

func testTruncate(t *testing.T, l *Log) {
	append := api.Record{
		Value: []byte("hello world"),
	}

	for i := 0; i < 3; i++ {
		_, err := l.Append(&append)
		if err != nil {
			t.Fatal(err)
		}
	}

	t.Logf("Length before truncation: %v", len(l.segments))
	err := l.Truncate(1)
	if err != nil {
		t.Fatal(err)
	}

	_, err = l.Read(0)
	if err == nil {
		t.Fatal("err on truncate ")
	}
	t.Log(err)
	t.Logf("Length after truncation: %v", len(l.segments))
}

func testReader(t *testing.T, l *Log) {
	append := api.Record{
		Value: []byte("hello world"),
	}

	off, err := l.Append(&append)
	if err != nil {
		t.Fatal(err)
	}

	if off != 0 {
		t.Fatalf("expected offset: 0, received: %v", off)
	}

	reader := l.Reader()
	b, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}

	read := &api.Record{}
	err = proto.Unmarshal(b[lenWidth:], read)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(append.Value, read.Value) {
		t.Fatal(err)
	}
}

func testOutOfRange(t *testing.T, l *Log) {

	offset := uint64(1)
	record, err := l.Read(offset)
	if record != nil {
		t.Fatal("Record should have been nil")
	}

	if err == nil {
		t.Fatal("Err should have been set")
	}

	apiErr := err.(api.ErrOffsetOutOfRange)
	if apiErr.Offset != offset {
		t.Fatalf("Err offset should have been %v, received: %v", offset, apiErr.Offset)
	}
}

func testInitExisting(t *testing.T, l *Log) {
	append := api.Record{
		Value: []byte("hello world"),
	}

	for i := 0; i < 3; i++ {
		_, err := l.Append(&append)
		if err != nil {
			t.Fatal(err)
		}
	}

	err := l.Close()
	if err != nil {
		t.Fatal(err)
	}

	off, err := l.LowestOffset()
	if err != nil {
		t.Fatal(err)
	}
	if off != 0 {
		t.Fatalf("expected offset 0, received %v", off)
	}

	off, err = l.HighestOffset()
	if err != nil {
		t.Fatal(err)
	}
	if off != 2 {
		t.Fatalf("expected offset 2, received %v", off)
	}

	n, err := NewLog(l.Dir, l.Config)
	if err != nil {
		t.Fatal(err)
	}

	off, err = n.LowestOffset()
	if err != nil {
		t.Fatal(err)
	}
	if off != 0 {
		t.Fatalf("expected offset 0, received %v", off)
	}

	off, err = n.HighestOffset()
	if err != nil {
		t.Fatal(err)
	}
	if off != 2 {
		t.Fatalf("expected offset 2, received %v", off)
	}
}
