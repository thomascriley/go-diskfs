package ext4

import (
	"testing"

	"github.com/go-test/deep"
)

func TestDirectoryEntriesFromBytes(t *testing.T) {
	expected, blockSize, b, err := testGetValidRootDirectory()
	if err != nil {
		t.Fatal(err)
	}
	// remove checksums, as we are not testing those here
	b = b[:len(b)-minDirEntryLength]
	// convert into directory entries
	entries := &directoryEntriesLinear{
		bytesPerBlock: blockSize,
		hasFileType:   true,
	}
	if err = entries.UnmarshalExt4(b); err != nil {
		t.Fatalf("Failed to parse directory entries: %v", err)
	}
	deep.CompareUnexportedFields = true
	if diff := deep.Equal(expected.entries, entries); diff != nil {
		t.Errorf("directoryFromBytes() = %v", diff)
	}
}
