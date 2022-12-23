package log

import (
	"bytes"
	"io"
	"os"
	"testing"

	api "github.com/cevataykans/proglog/api/v1"
)

func TestSegment(t *testing.T) {

	dir, err := os.MkdirTemp(os.TempDir(), "segment_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	want := &api.Record{
		Value: []byte("Hello World!"),
	}

	c := Config{}
	c.Segment.MaxIndexBytes = endWidth * 3
	c.Segment.MaxStoreBytes = 1024

	s, err := newSegment(dir, 16, c)
	if err != nil {
		t.Fatal(err)
	}

	if s.nextOffset != uint64(16) {
		t.Fatalf("nextoffset %v not equal to 16", s.nextOffset)
	}

	if s.IsMaxed() {
		t.Fatal("s should not be maxed out")
	}

	for i := uint64(0); i < 3; i++ {
		off, err := s.Append(want)
		if err != nil {
			t.Fatal(err)
		}

		if 16+i != off {
			t.Fatalf("Expected offset %v but got %v", 16+i, off)
		}

		got, err := s.Read(off)
		if err != nil {
			t.Fatal(err)
		}

		if !bytes.Equal(got.Value, want.Value) {
			t.Fatal("Writtten bytes does not equal to the read bytes")
		}
	}

	_, err = s.Append(want)
	if err != io.EOF {
		t.Fatalf("Expected EOF, received error: %v", err)
	}

	if !s.IsMaxed() {
		t.Fatal("Expected full, got false")
	}

	c.Segment.MaxStoreBytes = uint64(len(want.Value) * 3)
	c.Segment.MaxIndexBytes = 1024

	err = s.Close()
	if err != nil {
		t.Fatal(err)
	}

	s, err = newSegment(dir, 16, c)
	if err != nil {
		t.Fatal(err)
	}

	if !s.IsMaxed() {
		t.Fatal("Expected full, got not full")
	}

	err = s.Remove()
	if err != nil {
		t.Fatal(err)
	}

	s, err = newSegment(dir, 16, c)
	if err != nil {
		t.Fatal(err)
	}

	if s.IsMaxed() {
		t.Fatal("Expected empty, got full")
	}

	err = s.Close()
	if err != nil {
		t.Fatal(err)
	}
}
