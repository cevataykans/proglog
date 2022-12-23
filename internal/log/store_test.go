package log

import (
	"bytes"
	"os"
	"testing"
)

var (
	write = []byte("Hello is this working?")
	width = uint64(len(write)) + lenWidth
)

func TestStoreAppendRead(t *testing.T) {
	f, err := os.CreateTemp("", "store_append_read_test")
	if err != nil {
		t.Error(err)
	}
	defer os.Remove(f.Name())

	s, err := NewStore(f)
	if err != nil {
		t.Error(err)
	}

	testAppend(t, s)
	testRead(t, s)
	testReadAt(t, s)

	s, err = NewStore(f)
	if err != nil {
		t.Error(err)
	}
	testRead(t, s)
}

func TestStoreClose(t *testing.T) {

	f, err := os.CreateTemp("", "store_close_test")
	if err != nil {
		t.Error(err)
	}
	defer os.Remove(f.Name())

	s, err := NewStore(f)
	if err != nil {
		t.Error(err)
	}

	_, _, err = s.Append(write)
	if err != nil {
		t.Error(err)
	}

	f, beforeSize, err := openFile(f.Name())
	if err != nil {
		t.Error(err)
	}

	err = s.Close()
	if err != nil {
		t.Error(err)
	}

	_, afterSize, err := openFile(f.Name())
	if err != nil {
		t.Error(err)
	}

	if beforeSize >= afterSize {
		t.Errorf("expected afterSize: %v is smaller than before size: %v", afterSize, beforeSize)
	}
}

func testAppend(t *testing.T, s *store) {

	t.Helper()
	for i := uint64(1); i < 4; i++ {
		n, pos, err := s.Append(write)
		if err != nil {
			t.Error(err)
		}

		if n+pos != width*i {
			t.Errorf("amount written: %v, expected: %v", n+pos, width*i)
		}
	}
}

func testRead(t *testing.T, s *store) {

	t.Helper()
	var pos uint64
	for i := uint64(1); i < 4; i++ {
		b, err := s.Read(pos)
		if err != nil {
			t.Error(err)
		}

		if !bytes.Equal(b, write) {
			t.Errorf("data is different, read: %v, expected: %v", b, write)
		}
		pos += width
	}
}

func testReadAt(t *testing.T, s *store) {
	t.Helper()
	for i, offset := uint64(1), int64(0); i < 4; i++ {
		b := make([]byte, lenWidth)
		n, err := s.ReadAt(b, offset)
		if err != nil {
			t.Error(err)
		}
		offset += int64(n)

		size := enc.Uint64(b)
		d := make([]byte, size)
		n, err = s.ReadAt(d, offset)
		if err != nil {
			t.Error(err)
		}

		if !bytes.Equal(d, write) {
			t.Errorf("data is different, read: %v, expected: %v", d, write)
		}
		offset += int64(n)
	}
}

func openFile(name string) (file *os.File, size int64, err error) {

	f, err := os.OpenFile(name,
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, 0, err
	}
	fi, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	return f, fi.Size(), nil
}
