package ext4

import (
	"encoding/binary"
	"github.com/diskfs/go-diskfs/filesystem/ext4/crc"
)

// checksumAppender is a function that takes a byte slice and returns a byte slice with a checksum appended
type checksumAppender func([]byte)
type checksummer func(block []byte) uint32
type checksummerHash func(block []byte, tail []byte) uint32

// linearDirectoryCheckSum returns a function that implements checksumAppender for a directory entries block
// original calculations can be seen for e2fsprogs https://git.kernel.org/pub/scm/fs/ext2/e2fsprogs.git/tree/lib/ext2fs/csum.c#n301
// and in the linux tree https://github.com/torvalds/linux/blob/master/fs/ext4/namei.c#L376-L384
func linearDirectoryCheckSum(seed, inodeNumber, inodeGeneration uint32) checksummer {
	return func(b []byte) uint32 {
		numBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(numBytes, inodeNumber)
		crcResult := crc.CRC32c(seed, numBytes)
		genBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(genBytes, inodeGeneration)
		crcResult = crc.CRC32c(crcResult, genBytes)
		return crc.CRC32c(crcResult, b)
	}
}

// linearDirectoryCheckSum returns a function that implements checksumAppender for a directory entries block
// original calculations can be seen for e2fsprogs https://git.kernel.org/pub/scm/fs/ext2/e2fsprogs.git/tree/lib/ext2fs/csum.c#n301
// and in the linux tree https://github.com/torvalds/linux/blob/master/fs/ext4/namei.c#L376-L384
func hashTreeDirectoryCheckSum(seed, inodeNumber, inodeGeneration uint32) checksummerHash {
	return func(b []byte, tail []byte) uint32 {
		numBytes := make([]byte, 4)
		genBytes := make([]byte, 4)
		bs := make([]byte, len(b))
		tailBytes := make([]byte, 4)

		binary.LittleEndian.PutUint32(numBytes, inodeNumber)
		binary.LittleEndian.PutUint32(genBytes, inodeGeneration)
		copy(bs, b)
		copy(tailBytes, tail[:0x4])

		crcResult := crc.CRC32c(seed, numBytes)
		crcResult = crc.CRC32c(crcResult, genBytes)
		crcResult = crc.CRC32c(crcResult, bs) // meta data and dir entries
		crcResult = crc.CRC32c(crcResult, tailBytes)
		return crc.CRC32c(crcResult, make([]byte, 4))
	}
}

// directoryChecksumAppender returns a function that implements checksumAppender for a directory entries block
// original calculations can be seen for e2fsprogs https://git.kernel.org/pub/scm/fs/ext2/e2fsprogs.git/tree/lib/ext2fs/csum.c#n301
// and in the linux tree https://github.com/torvalds/linux/blob/master/fs/ext4/namei.c#L376-L384
//
//nolint:unparam // inodeGeneration is always 0
func directoryChecksumAppender(seed, inodeNumber, inodeGeneration uint32) checksumAppender {
	fn := linearDirectoryCheckSum(seed, inodeNumber, inodeGeneration)
	return func(b []byte) {
		binary.LittleEndian.PutUint32(b[len(b)-0x4:], fn(b[:len(b)-minDirEntryLength]))
	}
}

func directoryHashTreeChecksumAppender(seed, inodeNumber, inodeGeneration uint32) checksumAppender {
	fn := linearDirectoryCheckSum(seed, inodeNumber, inodeGeneration)
	return func(b []byte) {
		binary.LittleEndian.PutUint32(b[len(b)-0x8:], 0)
		binary.LittleEndian.PutUint32(b[len(b)-0x4:], fn(b))
	}
}

// nullDirectoryChecksummer does not change anything
func nullDirectoryChecksummer(b []byte) []byte {
	return b
}
