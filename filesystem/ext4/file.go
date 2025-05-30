package ext4

import (
	"fmt"
	"io"
	"io/fs"
	"os"
)

// File represents a single file in an ext4 filesystem
type File struct {
	*directoryEntry
	*inode
	isReadWrite bool
	isWriteOnly bool
	isAppend    bool
	offsetRead  int64
	offsetWrite int64
	filesystem  *FileSystem
	extents     extents
}

func (fl *File) Stat() (fs.FileInfo, error) {
	info := &FileInfo{
		modTime: fl.inode.modifyTime,
		name:    fl.filename,
		size:    int64(fl.size),
		isDir:   fl.directoryEntry.fileType == dirFileTypeDirectory,
	}
	switch fl.directoryEntry.fileType {
	case dirFileTypeDirectory:
		info.mode |= os.ModeDir
	case dirFileTypeBlock:
		info.mode |= os.ModeDevice
	case dirFileTypeCharacter:
		info.mode |= os.ModeCharDevice
	case dirFileTypeFifo:
		info.mode |= os.ModeNamedPipe
	case dirFileTypeRegular:
	case dirFileTypeSocket:
		info.mode |= os.ModeSocket
	case dirFileTypeSymlink:
		info.mode |= os.ModeSymlink
	}

	return info, nil
}

// Read reads up to len(b) bytes from the File.
// It returns the number of bytes read and any error encountered.
// At end of file, Read returns 0, io.EOF
// reads from the last known offset in the file from last read or write
// use Seek() to set at a particular point
func (fl *File) Read(b []byte) (int, error) {
	var (
		fileSize  = int64(fl.size)
		blocksize = uint64(fl.filesystem.superblock.blockSize)
	)
	if fl.offsetRead >= fileSize {
		return 0, io.EOF
	}

	// Calculate the number of bytes to read
	bytesToRead := int64(len(b))
	if fl.offsetRead+bytesToRead > fileSize {
		bytesToRead = fileSize - fl.offsetRead
	}

	// Create a buffer to hold the bytes to be read
	readBytes := int64(0)
	b = b[:bytesToRead]

	// the offset given for reading is relative to the file, so we need to calculate
	// where these are in the extents relative to the file
	readStartBlock := uint64(fl.offsetRead) / blocksize
	for _, e := range fl.extents {
		// if the last block of the extent is before the first block we want to read, skip it
		if uint64(e.fileBlock)+uint64(e.count) <= readStartBlock {
			continue
		}
		// extentSize is the number of bytes on the disk for the extent
		extentSize := int64(e.count) * int64(blocksize)
		// where do we start and end in the extent?
		startPositionInExtent := fl.offsetRead - int64(e.fileBlock)*int64(blocksize)
		leftInExtent := extentSize - startPositionInExtent
		// how many bytes are left to read
		toReadInOffset := bytesToRead - readBytes
		if toReadInOffset > leftInExtent {
			toReadInOffset = leftInExtent
		}
		// read those bytes
		startPosOnDisk := e.startingBlock*blocksize + uint64(startPositionInExtent) + uint64(fl.filesystem.start)
		b2 := make([]byte, toReadInOffset)
		read, err := fl.filesystem.file.ReadAt(b2, int64(startPosOnDisk))
		if err != nil {
			return int(readBytes), fmt.Errorf("failed to read bytes: %v", err)
		}
		copy(b[readBytes:], b2[:read])
		readBytes += int64(read)
		fl.offsetRead += int64(read)

		if readBytes >= bytesToRead {
			break
		}
	}
	var err error
	if fl.offsetRead >= fileSize {
		err = io.EOF
	}

	return int(readBytes), err
}

// Write writes len(b) bytes to the File.
// It returns the number of bytes written and an error, if any.
// returns a non-nil error when n != len(b)
// writes to the last known offset in the file from last read or write
// use Seek() to set at a particular point
func (fl *File) Write(b []byte) (int, error) {
	var (
		err                error
		written            int
		existingFileSize   = int64(fl.size)
		existingBlockCount = fl.blocks
		blockSize          = uint64(fl.filesystem.superblock.blockSize)
	)
	if !fl.isReadWrite && !fl.isWriteOnly && !fl.isAppend {
		return 0, fmt.Errorf("file is not open for writing")
	}

	// if appending keep existing bytes and set the offset to the end of the file
	// else truncate the size to the current offset. This will be zero on first write
	switch fl.directoryEntry.fileType {
	case dirFileTypeDirectory:
	case dirFileTypeRegular:
	default:
		return 0, fmt.Errorf("expecting: %d, received: %d", dirFileTypeRegular, fl.directoryEntry.fileType)
	}

	// if adding these bytes goes past the filesize, update the inode filesize to the new size and write the inode
	// if adding these bytes goes past the total number of blocks, add more blocks, update the inode block count and write the inode
	// if the offset is greater than the filesize, update the inode filesize to the offset

	// Calculate the number of bytes to write
	bytesToWrite := int64(len(b))

	// calculate the new size of the file
	fl.size = uint64(fl.offsetWrite + bytesToWrite)

	// calculate the number of blocks in the file post-write
	fl.blocks = fl.size / blockSize
	if fl.size%blockSize > 0 {
		fl.blocks++
	}
	if fl.blocks > existingBlockCount {
		// determine the number of leaf and internal node blocks needed
		existing := len(fl.extents)
		// allocateExtents removes existing extents blocks from the bytes needed internally
		if err = fl.filesystem.allocateExtents(fl.blocks*blockSize, &fl.extents); err != nil {
			return 0, fmt.Errorf("could not allocate disk space for file %w", err)
		}
		if existing < len(fl.extents) {
			if fl.inode.extents, err = extendExtentTree(fl.inode.extents, fl.extents[existing:], fl.filesystem, nil); err != nil {
				return 0, fmt.Errorf("could not convert extents into tree: %w", err)
			}
		}
	}

	if existingFileSize != int64(fl.size) || existingBlockCount != fl.blocks {
		if err = fl.filesystem.writeInode(fl.inode); err != nil {
			return 0, fmt.Errorf("could not write inode: %w", err)
		}
	}

	writtenBytes := int64(0)

	// the offset given for reading is relative to the file, so we need to calculate
	// where these are in the extents relative to the file
	writeStartBlock := uint64(fl.offsetWrite) / blockSize
	for _, e := range fl.extents {
		// if the last block of the extent is before the first block we want to write, skip it
		if uint64(e.fileBlock)+uint64(e.count) <= writeStartBlock {
			continue
		}
		// extentSize is the number of bytes on the disk for the extent
		extentSize := int64(e.count) * int64(blockSize)
		// where do we start and end in the extent?
		startPositionInExtent := fl.offsetWrite - int64(e.fileBlock)*int64(blockSize)
		leftInExtent := extentSize - startPositionInExtent
		// how many bytes are left in the extent?
		toWriteInOffset := bytesToWrite - writtenBytes
		if toWriteInOffset > leftInExtent {
			toWriteInOffset = leftInExtent
		}
		// read those bytes
		startPosOnDisk := e.startingBlock*blockSize + uint64(startPositionInExtent) + uint64(fl.filesystem.start)
		if written, err = fl.filesystem.file.WriteAt(b[writtenBytes:writtenBytes+toWriteInOffset], int64(startPosOnDisk)); err != nil {
			return int(writtenBytes), fmt.Errorf("failed to write bytes: %v", err)
		}
		writtenBytes += int64(written)
		fl.offsetWrite += int64(written)

		if writtenBytes >= int64(len(b)) {
			break
		}
	}

	return int(writtenBytes), nil
}

// Seek set the offset to a particular point in the file
func (fl *File) Seek(offset int64, whence int) (int64, error) {
	newOffset := int64(0)
	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekEnd:
		newOffset = int64(fl.size) + offset
	case io.SeekCurrent:
		newOffset = fl.offsetRead + offset
	}
	if newOffset < 0 {
		return fl.offsetRead, fmt.Errorf("cannot set offset %d before start of file", offset)
	}
	fl.offsetRead = newOffset
	return fl.offsetRead, nil
}

// Close close a file that is being read
func (fl *File) Close() error {
	*fl = File{}
	return nil
}
