package ext4

import (
	"encoding/binary"
	"fmt"
	"io"
	"strings"
)

// directoryFileType uses different constants than the file type property in the inode
type directoryFileType uint8

const (
	dirEntryHeaderLength int = 0x8
	minDirEntryLength    int = 12 // actually 9 for 1-byte file length, but must be multiple of 4 bytes
	maxDirEntryLength    int = 263

	// directory file types
	dirFileTypeUnknown   directoryFileType = 0x0
	dirFileTypeRegular   directoryFileType = 0x1
	dirFileTypeDirectory directoryFileType = 0x2
	dirFileTypeCharacter directoryFileType = 0x3
	dirFileTypeBlock     directoryFileType = 0x4
	dirFileTypeFifo      directoryFileType = 0x5
	dirFileTypeSocket    directoryFileType = 0x6
	dirFileTypeSymlink   directoryFileType = 0x7
)

type directoryEntries interface {
	marshaler
	unmarshaler
	Entries() []*directoryEntry
	AddEntry(entry *directoryEntry)
	RemoveEntry(entry *directoryEntry)
}

func directoryEntriesSize(entries directoryEntries) int {
	return entries.Size()
}

// directoryEntry is a single directory entry
type directoryEntry struct {
	inode       uint32
	length      uint16
	filename    string
	fileNameLen uint16
	fileType    directoryFileType

	// feature flags from superblock
	hasFileType bool
}

func (de *directoryEntry) equal(other *directoryEntry) bool {
	return de.inode == other.inode && de.filename == other.filename && de.fileType == other.fileType
}

func (de *directoryEntry) CalcSize() int {
	// it must be the header length + filename length rounded up to nearest multiple of 4
	nameLength := uint8(len(de.filename))
	entryLength := int(nameLength) + dirEntryHeaderLength
	if leftover := entryLength % 4; leftover > 0 {
		entryLength += 4 - leftover
	}
	return entryLength
}

func (de *directoryEntry) Size() int {
	entryLength := de.CalcSize()
	if l := int(de.length); l > entryLength {
		return l
	}
	return entryLength
}

// toBytes convert a directoryEntry to bytes. If isLast, then the size recorded is the number of bytes
// from beginning of directory entry to end of block, minus the amount left for the checksum.
func (de *directoryEntry) toBytes() []byte {
	b := make([]byte, de.length)
	_ = de.MarshalExt4(b)
	return b
}

func (de *directoryEntry) UnmarshalExt4(b []byte) (err error) {
	if err = de.UnmarshalExt4Header(b); err != nil {
		return fmt.Errorf("failed to deserialize header: %w", err)
	}
	return de.UnmarshalExt4FileName(b[dirEntryHeaderLength:])
}

func (de *directoryEntry) UnmarshalExt4Header(b []byte) (err error) {
	var offset int
	if offset, err = toUint32(b, 0x0, &de.inode); err != nil {
		return fmt.Errorf("failed to deserialize inode: %w", err)
	}
	if offset, err = toUint16(b, offset, &de.length); err != nil {
		return fmt.Errorf("failed to deserialize length: %w", err)
	}
	if de.hasFileType {
		var short uint8
		if offset, err = toUint8(b, offset, &short); err != nil {
			return fmt.Errorf("failed to deserialize file name length: %w", err)
		}
		de.fileNameLen = uint16(short)
		if offset, err = toDirectoryFileType(b, offset, &de.fileType); err != nil {
			return fmt.Errorf("failed to deserialize file type: %w", err)
		}
	} else {
		if offset, err = toUint16(b, offset, &de.fileNameLen); err != nil {
			return fmt.Errorf("failed to deserialize file name length: %w", err)
		}
	}
	return nil
}

func (de *directoryEntry) UnmarshalExt4FileName(b []byte) (err error) {
	if maxLen := uint16(maxDirEntryLength - dirEntryHeaderLength); de.fileNameLen > maxLen {
		de.fileNameLen = maxLen
	}
	if _, err = toString(b, 0, int(de.fileNameLen), &de.filename); err != nil {
		return fmt.Errorf("failed to deserialize file name: %w", err)
	}
	return err
}

func (de *directoryEntry) UnmarshalExt4From(reader io.Reader) (err error) {
	bs := make([]byte, dirEntryHeaderLength)
	if _, err = reader.Read(bs); err != nil {
		return err
	}
	if err = de.UnmarshalExt4Header(bs); err != nil {
		return err
	}
	bs = make([]byte, de.fileNameLen)
	if _, err = reader.Read(bs); err != nil {
		return err
	}
	if err = de.UnmarshalExt4FileName(bs[dirEntryHeaderLength:]); err != nil {
		return err
	}
	return nil
}

func (de *directoryEntry) MarshalExt4(b []byte) (err error) {
	// calc size is the actual number of bytes needed to write all the information including the filename
	// calc size can be different than de.length as de.length may need to extend to the end of the block
	if len(b) < de.CalcSize() {
		return fmt.Errorf("directory entry of bytes of length %d is too short for the calculated size %d", len(b), de.CalcSize())
	}
	if int(de.length) < minDirEntryLength {
		return fmt.Errorf("the directory entry length %d, is less than the minimum size %d", de.length, minDirEntryLength)
	}
	fileNameLen := uint16(len(de.filename))
	binary.LittleEndian.PutUint32(b[0x0:0x4], de.inode)

	// assumed that when the filename changes or the entry is created, this value is changed and the entire directory block is rewritten
	binary.LittleEndian.PutUint16(b[0x4:0x6], de.length)

	if de.hasFileType {
		b[0x6] = uint8(fileNameLen)
		b[0x7] = byte(de.fileType)
	} else {
		binary.LittleEndian.PutUint16(b[0x6:0x8], fileNameLen)
	}
	copy(b[0x8:], de.filename)
	return nil
}

func (de *directoryEntry) MarshalExt4To(writer io.Writer) (err error) {
	buf := make([]byte, 4)
	fileNameLen := uint16(len(de.filename))
	binary.LittleEndian.PutUint32(buf[:0x4], de.inode)
	if _, err = writer.Write(buf[:0x4]); err != nil {
		return err
	}
	binary.LittleEndian.PutUint16(buf[:0x2], uint16(de.Size()))
	if _, err = writer.Write(buf[:0x2]); err != nil {
		return err
	}
	if de.hasFileType {
		if _, err = writer.Write([]byte{uint8(fileNameLen), byte(de.fileType)}); err != nil {
			return err
		}
	} else {
		binary.LittleEndian.PutUint16(buf[:0x2], de.fileNameLen)
		if _, err = writer.Write(buf[:0x2]); err != nil {
			return err
		}
	}
	_, err = io.Copy(writer, strings.NewReader(de.filename))
	return err
}
