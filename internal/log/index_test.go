package log

import (
	"io"
	"os"
	"testing"
)

func TestIndex(t *testing.T) {

	f, err := os.CreateTemp(os.TempDir(), "index_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())

	c := Config{}
	c.Segment.MaxIndexBytes = 1024

	idx, err := NewIndex(f, c)
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = idx.Read(-1)
	if err != io.EOF {
		t.Fatal("Expected EOF, got nil err")
	}

	if idx.Name() != f.Name() {
		t.Fatalf("filename %v does not match filename %v", idx.Name(), f.Name())
	}

	entries := []struct {
		Off uint32
		Pos uint64
	}{
		{Off: 0, Pos: 0},
		{Off: 1, Pos: 10},
	}

	for _, want := range entries {

		err = idx.Write(want.Off, want.Pos)
		if err != nil {
			t.Fatal(err)
		}

		_, readPos, err := idx.Read(int64(want.Off))
		if err != nil {
			t.Fatal(err)
		}
		if readPos != want.Pos {
			t.Fatal("err read pos and write pos do not match")
		}
	}

	_, _, err = idx.Read(int64(len(entries)))
	if err != io.EOF {
		t.Fatal("err is not equal to the EOF")
	}

	if err := idx.Close(); err != nil {
		t.Fatal(err)
	}

	f, err = os.OpenFile(f.Name(), os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}

	idx, err = NewIndex(f, c)
	if err != nil {
		t.Fatal(err)
	}

	readOff, readPos, err := idx.Read(-1)
	if err != nil {
		t.Fatal(err)
	}

	if readPos != entries[1].Pos || readOff != uint32(1) {
		t.Fatal("reconstructed idx does nto work as expected!")
	}

	if err := idx.Close(); err != nil {
		t.Fatal(err)
	}
}
