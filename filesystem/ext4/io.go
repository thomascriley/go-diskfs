package ext4

import (
	"encoding/binary"
	"fmt"
	"io"
)

func toUint32(b []byte, start int, to *uint32) (int, error) {
	if len(b) < start+4 {
		return 0, fmt.Errorf("%w: expected at least %d bytes, received: %d", io.EOF, start+4, len(b))
	}
	*to = binary.LittleEndian.Uint32(b[start:])
	return start + 4, nil
}

func toUint16(b []byte, start int, to *uint16) (int, error) {
	if len(b) < start+2 {
		return 0, fmt.Errorf("%w: expected at least %d bytes, received: %d", io.EOF, start+2, len(b))
	}
	*to = binary.LittleEndian.Uint16(b[start:])
	return start + 2, nil
}

func toUint8(b []byte, start int, to *uint8) (int, error) {
	if len(b) <= start {
		return 0, fmt.Errorf("%w: expected at least %d bytes, received: %d", io.EOF, 1, len(b))
	}
	*to = b[start]
	return start + 1, nil
}

func toDirectoryFileType(b []byte, start int, to *directoryFileType) (int, error) {
	if len(b) <= start {
		return 0, fmt.Errorf("%w: expected at least %d bytes, received: %d", io.EOF, 1, len(b))
	}
	*to = directoryFileType(b[start])
	return start + 1, nil
}

func toString(b []byte, start, length int, to *string) (int, error) {
	if len(b) < start+length {
		return 0, fmt.Errorf("%w: expected at least %d bytes, received: %d", io.EOF, 1, len(b))
	}
	*to = string(b[start : start+length])
	return start + length, nil
}
