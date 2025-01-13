package diskfs

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
	"syscall"
	"unsafe"
)

// getBlockDeviceSize get the size of an opened block device in Bytes.
func getBlockDeviceSize(f *os.File) (int64, error) {
	//blockDeviceSize, err := unix.IoctlGetInt(int(f.Fd()), unix.BLKGETSIZE64)
	var blockDeviceSize uint64
	if _, _, err := syscall.Syscall(syscall.SYS_IOCTL, f.Fd(), unix.BLKGETSIZE64, uintptr(unsafe.Pointer(&blockDeviceSize))); err != 0 {
		return 0, os.NewSyscallError("ioctl: BLKGETSIZE64", err)
	}
	return int64(blockDeviceSize), nil
}

// getSectorSizes get the logical and physical sector sizes for a block device
func getSectorSizes(f *os.File) (logicalSectorSize, physicalSectorSize int64, err error) {
	//
	//  equivalent syscall to
	//    ioctl(fd, BLKPBSZGET, &physicalsectsize);
	fd := f.Fd()

	logicalSectorSizeInt, err := unix.IoctlGetInt(int(fd), unix.BLKSSZGET)
	if err != nil {
		return 0, 0, fmt.Errorf("unable to get device logical sector size: %v", err)
	}
	physicalSectorSizeInt, err := unix.IoctlGetInt(int(fd), unix.BLKPBSZGET)
	if err != nil {
		return 0, 0, fmt.Errorf("unable to get device physical sector size: %v", err)
	}
	return int64(logicalSectorSizeInt), int64(physicalSectorSizeInt), nil
}
