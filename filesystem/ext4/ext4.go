package ext4

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/diskfs/go-diskfs/filesystem"
	"github.com/diskfs/go-diskfs/filesystem/ext4/crc"
	"github.com/diskfs/go-diskfs/util"
	"github.com/google/uuid"
)

type marshaler interface {
	Size() int
	MarshalExt4(b []byte) error
}

type unmarshaler interface {
	UnmarshalExt4([]byte) error
}

// SectorSize indicates what the sector size in bytes is
type SectorSize uint16

// BlockSize indicates how many sectors are in a block
type BlockSize uint8

// BlockGroupSize indicates how many blocks are in a group, standardly 8*block_size_in_bytes

const (
	// SectorSize512 is a sector size of 512 bytes, used as the logical size for all ext4 filesystems
	SectorSize512                SectorSize = 512
	minBlocksPerGroup            uint32     = 256
	BootSectorSize               SectorSize = 2 * SectorSize512
	SuperblockSize               SectorSize = 2 * SectorSize512
	BlockGroupFactor             int        = 8
	DefaultInodeRatio            int64      = 8192
	DefaultInodeSize             int64      = 256
	DefaultReservedBlocksPercent uint8      = 5
	DefaultVolumeName                       = "diskfs_ext4"
	minClusterSize               int        = 128
	maxClusterSize               int        = 65529
	bytesPerSlot                 int        = 32
	maxCharsLongFilename         int        = 13
	maxBlocksPerExtent           uint16     = 32768
	million                      int        = 1000000
	billion                      int        = 1000 * million
	firstNonReservedInode        uint32     = 11 // traditional

	minBlockLogSize int = 10 /* 1024 */
	maxBlockLogSize int = 16 /* 65536 */
	minBlockSize    int = 1 << minBlockLogSize
	maxBlockSize    int = 1 << maxBlockLogSize

	max32Num uint64 = math.MaxUint32
	max64Num uint64 = math.MaxUint64

	maxFilesystemSize32Bit uint64 = 16 << 40
	maxFilesystemSize64Bit uint64 = 1 << 60

	checksumType uint8 = 1

	// default for log groups per flex group
	defaultLogGroupsPerFlex int = 3

	// fixed inodes
	rootInode       uint32 = 2
	userQuotaInode  uint32 = 3
	groupQuotaInode uint32 = 4
	journalInode    uint32 = 8
	lostFoundInode         = 11 // traditional
)

type Params struct {
	UUID                  *uuid.UUID
	SectorsPerBlock       uint8
	BlocksPerGroup        uint32
	InodeRatio            int64
	InodeCount            uint32
	SparseSuperVersion    uint8
	Checksum              bool
	ClusterSize           int64
	ReservedBlocksPercent uint8
	VolumeName            string
	// JournalDevice external journal device, only checked if WithFeatureSeparateJournalDevice(true) is set
	JournalDevice      string
	LogFlexBlockGroups int
	Features           []FeatureOpt
	DefaultMountOpts   []MountOpt
}

// FileSystem implememnts the FileSystem interface
type FileSystem struct {
	bootSector       []byte
	superblock       *superblock
	groupDescriptors *groupDescriptors
	blockGroups      int64
	size             int64
	start            int64
	file             util.File
}

// Equal compare if two filesystems are equal
func (fs *FileSystem) Equal(a *FileSystem) bool {
	localMatch := fs.file == a.file
	sbMatch := fs.superblock.equal(a.superblock)
	gdMatch := fs.groupDescriptors.equal(a.groupDescriptors)
	return localMatch && sbMatch && gdMatch
}

// Create creates an ext4 filesystem in a given file or device
//
// requires the util.File where to create the filesystem, size is the size of the filesystem in bytes,
// start is how far in bytes from the beginning of the util.File to create the filesystem,
// and sectorsize is the logical sector size to use for creating the filesystem
//
// blocksize is the size of the ext4 blocks, and is calculated as sectorsPerBlock * sectorsize.
// By ext4 specification, it must be between 512 and 4096 bytes,
// where sectorsize is the provided parameter, and sectorsPerBlock is part of `p *Params`.
// If either sectorsize or p.SectorsPerBlock is 0, it will calculate the optimal size for both.
//
// note that you are *not* required to create the filesystem on the entire disk. You could have a disk of size
// 20GB, and create a small filesystem of size 50MB that begins 2GB into the disk.
// This is extremely useful for creating filesystems on disk partitions.
//
// Note, however, that it is much easier to do this using the higher-level APIs at github.com/diskfs/go-diskfs
// which allow you to work directly with partitions, rather than having to calculate (and hopefully not make any errors)
// where a partition starts and ends.
//
// If the provided blocksize is 0, it will use the default of 512 bytes. If it is any number other than 0
// or 512, it will return an error.
//
//nolint:gocyclo // yes, this has high cyclomatic complexity, but we can accept it
func Create(f util.File, size, start, sectorsize int64, p *Params) (*FileSystem, error) {
	// be safe about the params pointer
	if p == nil {
		p = &Params{}
	}

	// sectorsize must be <=0 or exactly SectorSize512 or error
	// because of this, we know we can scale it down to a uint32, since it only can be 512 bytes
	if sectorsize != int64(SectorSize512) && sectorsize > 0 {
		return nil, fmt.Errorf("sectorsize for ext4 must be either 512 bytes or 0, not %d", sectorsize)
	}
	if sectorsize == 0 {
		sectorsize = int64(SectorSize512)
	}
	var sectorsize32 = uint32(sectorsize)
	// there almost are no limits on an ext4 fs - theoretically up to 1 YB
	// but we do have to check the max and min size per the requested parameters
	//   if size < minSizeGivenParameters {
	// 	    return nil, fmt.Errorf("requested size is smaller than minimum allowed ext4 size %d for given parameters", minSizeGivenParameters*4)
	//   }
	//   if size > maxSizeGivenParameters {
	//	     return nil, fmt.Errorf("requested size is bigger than maximum ext4 size %d for given parameters", maxSizeGivenParameters*4)
	//   }

	// uuid
	fsuuid := p.UUID
	if fsuuid == nil {
		fsuuid2, _ := uuid.NewRandom()
		fsuuid = &fsuuid2
	}

	// blocksize
	sectorsPerBlock := p.SectorsPerBlock
	// whether or not the user provided a blocksize
	// if they did, we will stick with it, as long as it is valid.
	// if they did not, then we are free to calculate it
	var userProvidedBlocksize bool
	switch {
	case sectorsPerBlock == 0:
		sectorsPerBlock = 2
		userProvidedBlocksize = false
	case sectorsPerBlock > 128 || sectorsPerBlock < 2:
		return nil, fmt.Errorf("invalid sectors per block %d, must be between %d and %d sectors", sectorsPerBlock, 2, 128)
	default:
		userProvidedBlocksize = true
	}
	blocksize := uint32(sectorsPerBlock) * sectorsize32

	// how many whole blocks is that?
	numblocks := size / int64(blocksize)

	// recalculate if it was not user provided
	if !userProvidedBlocksize {
		sectorsPerBlockR, blocksizeR, numblocksR := recalculateBlockSize(numblocks, size)
		_, blocksize, numblocks = uint8(sectorsPerBlockR), blocksizeR, numblocksR
	}

	// how many blocks in each block group (and therefore how many block groups)
	// if not provided, by default it is 8*blocksize (in bytes)
	blocksPerGroup := p.BlocksPerGroup
	switch {
	case blocksPerGroup <= 0:
		blocksPerGroup = blocksize * 8
	case blocksPerGroup < minBlocksPerGroup:
		return nil, fmt.Errorf("invalid number of blocks per group %d, must be at least %d", blocksPerGroup, minBlocksPerGroup)
	case blocksPerGroup > 8*blocksize:
		return nil, fmt.Errorf("invalid number of blocks per group %d, must be no larger than 8*blocksize of %d", blocksPerGroup, blocksize)
	case blocksPerGroup%8 != 0:
		return nil, fmt.Errorf("invalid number of blocks per group %d, must be divisible by 8", blocksPerGroup)
	}

	// how many block groups do we have?
	blockGroups := numblocks / int64(blocksPerGroup)

	// track how many free blocks we have
	freeBlocks := numblocks

	clusterSize := p.ClusterSize

	// use our inode ratio to determine how many inodes we should have
	inodeRatio := p.InodeRatio
	if inodeRatio <= 0 {
		inodeRatio = DefaultInodeRatio
	}
	if inodeRatio < int64(blocksize) {
		inodeRatio = int64(blocksize)
	}
	if inodeRatio < clusterSize {
		inodeRatio = clusterSize
	}

	inodeCount := p.InodeCount
	switch {
	case inodeCount <= 0:
		// calculate how many inodes are needed
		inodeCount64 := (numblocks * int64(blocksize)) / inodeRatio
		if uint64(inodeCount64) > max32Num {
			return nil, fmt.Errorf("requested %d inodes, greater than max %d", inodeCount64, max32Num)
		}
		inodeCount = uint32(inodeCount64)
	case uint64(inodeCount) > max32Num:
		return nil, fmt.Errorf("requested %d inodes, greater than max %d", inodeCount, max32Num)
	}

	inodesPerGroup := int64(inodeCount) / blockGroups

	// track how many free inodes we have
	freeInodes := inodeCount

	// which blocks have superblock and GDT?
	var (
		backupSuperblocks            []int64
		backupSuperblockGroupsSparse [2]uint32
	)
	//  0 - primary
	//  ?? - backups
	switch p.SparseSuperVersion {
	case 2:
		// backups in first and last block group
		backupSuperblockGroupsSparse = [2]uint32{0, uint32(blockGroups) - 1}
		backupSuperblocks = []int64{0, 1, blockGroups - 1}
	default:
		backupSuperblockGroups := calculateBackupSuperblockGroups(blockGroups)
		backupSuperblocks = []int64{0}
		for _, bg := range backupSuperblockGroups {
			backupSuperblocks = append(backupSuperblocks, bg*int64(blocksPerGroup))
		}
	}

	freeBlocks -= int64(len(backupSuperblocks))

	var firstDataBlock uint32
	if blocksize == 1024 {
		firstDataBlock = 1
	}

	/*
		size calculations
		we have the total size of the disk from `size uint64`
		we have the sectorsize fixed at SectorSize512

		what do we need to determine or calculate?
		- block size
		- number of blocks
		- number of block groups
		- block groups for superblock and gdt backups
		- in each block group:
				- number of blocks in gdt
				- number of reserved blocks in gdt
				- number of blocks in inode table
				- number of data blocks

		config info:

		[defaults]
			base_features = sparse_super,large_file,filetype,resize_inode,dir_index,ext_attr
			default_mntopts = acl,user_xattr
			enable_periodic_fsck = 0
			blocksize = 4096
			inode_size = 256
			inode_ratio = 16384

		[fs_types]
			ext3 = {
				features = has_journal
			}
			ext4 = {
				features = has_journal,extent,huge_file,flex_bg,uninit_bg,64bit,dir_nlink,extra_isize
				inode_size = 256
			}
			ext4dev = {
				features = has_journal,extent,huge_file,flex_bg,uninit_bg,inline_data,64bit,dir_nlink,extra_isize
				inode_size = 256
				options = test_fs=1
			}
			small = {
				blocksize = 1024
				inode_size = 128
				inode_ratio = 4096
			}
			floppy = {
				blocksize = 1024
				inode_size = 128
				inode_ratio = 8192
			}
			big = {
				inode_ratio = 32768
			}
			huge = {
				inode_ratio = 65536
			}
			news = {
				inode_ratio = 4096
			}
			largefile = {
				inode_ratio = 1048576
				blocksize = -1
			}
			largefile4 = {
				inode_ratio = 4194304
				blocksize = -1
			}
			hurd = {
			     blocksize = 4096
			     inode_size = 128
			}
	*/

	// allocate root directory, single inode
	freeInodes--

	// how many reserved blocks?
	reservedBlocksPercent := p.ReservedBlocksPercent
	if reservedBlocksPercent <= 0 {
		reservedBlocksPercent = DefaultReservedBlocksPercent
	}

	// are checksums enabled?
	gdtChecksumType := gdtChecksumNone
	if p.Checksum {
		gdtChecksumType = gdtChecksumMetadata
	}

	// we do not yet support bigalloc
	var clustersPerGroup = blocksPerGroup

	// inodesPerGroup: once we know how many inodes per group, and how many groups
	//   we will have the total inode count

	volumeName := p.VolumeName
	if volumeName == "" {
		volumeName = DefaultVolumeName
	}

	fflags := defaultFeatureFlags
	for _, flagopt := range p.Features {
		flagopt(&fflags)
	}

	mflags := defaultMiscFlags

	// generate hash seed
	hashSeed, _ := uuid.NewRandom()
	hashSeedBytes := hashSeed[:]
	htreeSeed := make([]uint32, 0, 4)
	htreeSeed = append(htreeSeed,
		binary.LittleEndian.Uint32(hashSeedBytes[:4]),
		binary.LittleEndian.Uint32(hashSeedBytes[4:8]),
		binary.LittleEndian.Uint32(hashSeedBytes[8:12]),
		binary.LittleEndian.Uint32(hashSeedBytes[12:16]),
	)

	// create a UUID for the journal
	journalSuperblockUUID, _ := uuid.NewRandom()

	// group descriptor size could be 32 or 64, depending on option
	var gdSize uint16
	if fflags.fs64Bit {
		gdSize = groupDescriptorSize64Bit
	}

	var firstMetaBG uint32
	if fflags.metaBlockGroups {
		return nil, fmt.Errorf("meta block groups not yet supported")
	}

	// calculate the maximum number of block groups
	// maxBlockGroups = (maxFSSize) / (blocksPerGroup * blocksize)
	var (
		maxBlockGroups uint64
	)
	if fflags.fs64Bit {
		maxBlockGroups = maxFilesystemSize64Bit / (uint64(blocksPerGroup) * uint64(blocksize))
	} else {
		maxBlockGroups = maxFilesystemSize32Bit / (uint64(blocksPerGroup) * uint64(blocksize))
	}
	reservedGDTBlocks := maxBlockGroups * 32 / maxBlockGroups
	if reservedGDTBlocks > math.MaxUint16 {
		return nil, fmt.Errorf("too many reserved blocks calculated for group descriptor table")
	}

	var (
		journalDeviceNumber uint32
		err                 error
	)
	if fflags.separateJournalDevice && p.JournalDevice != "" {
		journalDeviceNumber, err = journalDevice(p.JournalDevice)
		if err != nil {
			return nil, fmt.Errorf("unable to get journal device: %w", err)
		}
	}

	// get default mount options
	mountOptions := defaultMountOptionsFromOpts(p.DefaultMountOpts)

	// initial KB written. This must be adjusted over time to include:
	// - superblock itself (1KB bytes)
	// - GDT
	// - block bitmap (1KB per block group)
	// - inode bitmap (1KB per block group)
	// - inode tables (inodes per block group * bytes per inode)
	// - root directory

	// for now, we just make it 1024 = 1 KB
	initialKB := 1024

	// only set a project quota inode if the feature was enabled
	var projectQuotaInode uint32
	if fflags.projectQuotas {
		projectQuotaInode = lostFoundInode + 1
		freeInodes--
	}

	// how many log groups per flex group? Depends on if we have flex groups
	logGroupsPerFlex := 0
	if fflags.flexBlockGroups {
		logGroupsPerFlex = defaultLogGroupsPerFlex
		if p.LogFlexBlockGroups > 0 {
			logGroupsPerFlex = p.LogFlexBlockGroups
		}
	}

	// create the superblock - MUST ADD IN OPTIONS
	now, epoch := time.Now(), time.Unix(0, 0)
	sb := superblock{
		inodeCount:                   inodeCount,
		blockCount:                   uint64(numblocks),
		reservedBlocks:               uint64(reservedBlocksPercent) / 100 * uint64(numblocks),
		freeBlocks:                   uint64(freeBlocks),
		freeInodes:                   freeInodes,
		firstDataBlock:               firstDataBlock,
		blockSize:                    blocksize,
		clusterSize:                  uint64(clusterSize),
		blocksPerGroup:               blocksPerGroup,
		clustersPerGroup:             clustersPerGroup,
		inodesPerGroup:               uint32(inodesPerGroup),
		mountTime:                    now,
		writeTime:                    now,
		mountCount:                   0,
		mountsToFsck:                 0,
		filesystemState:              fsStateCleanlyUnmounted,
		errorBehaviour:               errorsContinue,
		minorRevision:                0,
		lastCheck:                    now,
		checkInterval:                0,
		creatorOS:                    osLinux,
		revisionLevel:                1,
		reservedBlocksDefaultUID:     0,
		reservedBlocksDefaultGID:     0,
		firstNonReservedInode:        firstNonReservedInode,
		inodeSize:                    uint16(DefaultInodeSize),
		blockGroup:                   0,
		features:                     fflags,
		uuid:                         fsuuid,
		volumeLabel:                  volumeName,
		lastMountedDirectory:         "/",
		algorithmUsageBitmap:         0, // not used in Linux e2fsprogs
		preallocationBlocks:          0, // not used in Linux e2fsprogs
		preallocationDirectoryBlocks: 0, // not used in Linux e2fsprogs
		reservedGDTBlocks:            uint16(reservedGDTBlocks),
		journalSuperblockUUID:        &journalSuperblockUUID,
		journalInode:                 journalInode,
		journalDeviceNumber:          journalDeviceNumber,
		orphanedInodesStart:          0,
		hashTreeSeed:                 htreeSeed,
		hashVersion:                  hashHalfMD4,
		groupDescriptorSize:          gdSize,
		defaultMountOptions:          *mountOptions,
		firstMetablockGroup:          firstMetaBG,
		mkfsTime:                     now,
		journalBackup:                nil,
		// 64-bit mode features
		inodeMinBytes:                minInodeExtraSize,
		inodeReserveBytes:            wantInodeExtraSize,
		miscFlags:                    mflags,
		raidStride:                   0,
		multiMountPreventionInterval: 0,
		multiMountProtectionBlock:    0,
		raidStripeWidth:              0,
		checksumType:                 checksumType,
		totalKBWritten:               uint64(initialKB),
		errorCount:                   0,
		errorFirstTime:               epoch,
		errorFirstInode:              0,
		errorFirstBlock:              0,
		errorFirstFunction:           "",
		errorFirstLine:               0,
		errorLastTime:                epoch,
		errorLastInode:               0,
		errorLastLine:                0,
		errorLastBlock:               0,
		errorLastFunction:            "",
		mountOptions:                 "", // no mount options until it is mounted
		backupSuperblockBlockGroups:  backupSuperblockGroupsSparse,
		lostFoundInode:               lostFoundInode,
		overheadBlocks:               0,
		checksumSeed:                 crc.CRC32c(0, fsuuid[:]), // according to docs, this should be crc32c(~0, $orig_fs_uuid)
		snapshotInodeNumber:          0,
		snapshotID:                   0,
		snapshotReservedBlocks:       0,
		snapshotStartInode:           0,
		userQuotaInode:               userQuotaInode,
		groupQuotaInode:              groupQuotaInode,
		projectQuotaInode:            projectQuotaInode,
		logGroupsPerFlex:             uint64(logGroupsPerFlex),
	}
	gdt := groupDescriptors{}

	b, err := sb.toBytes()
	if err != nil {
		return nil, fmt.Errorf("error converting Superblock to bytes: %v", err)
	}

	g := gdt.toBytes(gdtChecksumType, sb.checksumSeed)
	// how big should the GDT be?
	gdSize = groupDescriptorSize
	if sb.features.fs64Bit {
		gdSize = groupDescriptorSize64Bit
	}
	gdtSize := int64(gdSize) * numblocks
	// write the superblock and GDT to the various locations on disk
	for _, bg := range backupSuperblocks {
		block := bg * int64(blocksPerGroup)
		blockStart := block * int64(blocksize)
		// allow that the first one requires an offset
		incr := int64(0)
		if block == 0 {
			incr = int64(SectorSize512) * 2
		}

		// write the superblock
		count, err := f.WriteAt(b, incr+blockStart+start)
		if err != nil {
			return nil, fmt.Errorf("error writing Superblock for block %d to disk: %v", block, err)
		}
		if count != int(SuperblockSize) {
			return nil, fmt.Errorf("wrote %d bytes of Superblock for block %d to disk instead of expected %d", count, block, SuperblockSize)
		}

		// write the GDT
		count, err = f.WriteAt(g, incr+blockStart+int64(SuperblockSize)+start)
		if err != nil {
			return nil, fmt.Errorf("error writing GDT for block %d to disk: %v", block, err)
		}
		if count != int(gdtSize) {
			return nil, fmt.Errorf("wrote %d bytes of GDT for block %d to disk instead of expected %d", count, block, gdtSize)
		}
	}

	// create root directory
	// there is nothing in there
	return &FileSystem{
		bootSector:       []byte{},
		superblock:       &sb,
		groupDescriptors: &gdt,
		blockGroups:      blockGroups,
		size:             size,
		start:            start,
		file:             f,
	}, nil
}

// Read reads a filesystem from a given disk.
//
// requires the util.File where to read the filesystem, size is the size of the filesystem in bytes,
// start is how far in bytes from the beginning of the util.File the filesystem is expected to begin,
// and blocksize is is the logical blocksize to use for creating the filesystem
//
// note that you are *not* required to read a filesystem on the entire disk. You could have a disk of size
// 20GB, and a small filesystem of size 50MB that begins 2GB into the disk.
// This is extremely useful for working with filesystems on disk partitions.
//
// Note, however, that it is much easier to do this using the higher-level APIs at github.com/diskfs/go-diskfs
// which allow you to work directly with partitions, rather than having to calculate (and hopefully not make any errors)
// where a partition starts and ends.
//
// If the provided blocksize is 0, it will use the default of 512 bytes. If it is any number other than 0
// or 512, it will return an error.
func Read(file util.File, size, start, sectorsize int64) (*FileSystem, error) {
	// blocksize must be <=0 or exactly SectorSize512 or error
	if sectorsize != int64(SectorSize512) && sectorsize > 0 {
		return nil, fmt.Errorf("sectorsize for ext4 must be either 512 bytes or 0, not %d", sectorsize)
	}
	// we do not check for ext4 max size because it is theoretically 1YB, which is bigger than an int64! Even 1ZB is!
	if size < Ext4MinSize {
		return nil, fmt.Errorf("requested size is smaller than minimum allowed ext4 size %d", Ext4MinSize)
	}

	// load the information from the disk
	// read boot sector code
	bs := make([]byte, BootSectorSize)
	n, err := file.ReadAt(bs, start)
	if err != nil {
		return nil, fmt.Errorf("could not read boot sector bytes from file: %v", err)
	}
	if uint16(n) < uint16(BootSectorSize) {
		return nil, fmt.Errorf("only could read %d boot sector bytes from file", n)
	}

	// read the superblock
	// the superblock is one minimal block, i.e. 2 sectors
	superblockBytes := make([]byte, SuperblockSize)
	n, err = file.ReadAt(superblockBytes, start+int64(BootSectorSize))
	if err != nil {
		return nil, fmt.Errorf("could not read superblock bytes from file: %v", err)
	}
	if uint16(n) < uint16(SuperblockSize) {
		return nil, fmt.Errorf("only could read %d superblock bytes from file", n)
	}

	// convert the bytes into a superblock structure
	sb, err := superblockFromBytes(superblockBytes)
	if err != nil {
		return nil, fmt.Errorf("could not interpret superblock data: %v", err)
	}

	// now read the GDT
	// how big should the GDT be?
	gdtSize := uint64(sb.groupDescriptorSize) * sb.blockGroupCount()

	gdtBytes := make([]byte, gdtSize)

	n, err = file.ReadAt(gdtBytes, start+getGDTBlock(sb)*int64(sb.blockSize))
	if err != nil {
		return nil, fmt.Errorf("could not read Group Descriptor Table bytes from file: %v", err)
	}
	if uint64(n) < gdtSize {
		return nil, fmt.Errorf("only could read %d Group Descriptor Table bytes from file instead of %d", n, gdtSize)
	}
	gdt, err := groupDescriptorsFromBytes(gdtBytes, sb.groupDescriptorSize, sb.checksumSeed, sb.gdtChecksumType())
	if err != nil {
		return nil, fmt.Errorf("could not interpret Group Descriptor Table data: %v", err)
	}

	return &FileSystem{
		bootSector:       bs,
		superblock:       sb,
		groupDescriptors: gdt,
		blockGroups:      int64(sb.blockGroupCount()),
		size:             size,
		start:            start,
		file:             file,
	}, nil
}

// Type returns the type code for the filesystem. Always returns filesystem.TypeExt4
func (fs *FileSystem) Type() filesystem.Type {
	return filesystem.TypeExt4
}

// Mkdir make a directory at the given path. It is equivalent to `mkdir -p`, i.e. idempotent, in that:
//
// * It will make the entire tree path if it does not exist
// * It will not return an error if the path already exists
func (fs *FileSystem) Mkdir(p string, mode os.FileMode) error {
	_, err := fs.readDirWithMkdir(p, true, mode|os.ModeDir)
	// we are not interested in returning the entries
	return err
}

// Symlink creates newname as a symbolic link to oldname. On Windows, a symlink to a non-existent oldname creates a file
// symlink; if oldname is later created as a directory the symlink will not work. If there is an error, it will be of
// type *LinkError.
func (fs *FileSystem) Symlink(oldName string, newName string) (err error) {
	if !path.IsAbs(newName) {
		return fmt.Errorf("the new name must be an absolute path: received %s", newName)
	}

	newName = path.Clean(newName)
	newFileName := path.Base(newName)
	newDirectory := path.Dir(newName)

	// is the symlink relative or absolute?
	if !path.IsAbs(oldName) {
		// convert it into an absolute path
		oldName = path.Join(newDirectory, oldName)
		oldName = path.Clean(oldName)
	}

	var (
		newParentDir *Directory
		newEntry     *directoryEntry
		oldEntry     *directoryEntry
	)

	// ensure the symlink does not exist
	if newParentDir, newEntry, err = fs.getEntryAndParent(newName); err != nil {
		return err
	} else if newEntry != nil {
		return fmt.Errorf("%w at new name %s", os.ErrExist, newName)
	}

	// ensure the target of the symlink exists
	if _, oldEntry, err = fs.getEntryAndParent(oldName); err != nil {
		return err
	} else if oldEntry == nil {
		return fmt.Errorf("%w at old name: %s", os.ErrNotExist, oldName)
	}

	// create the inode
	if _, err = fs.mkDirEntry(newParentDir, newFileName, oldName, os.ModeSymlink); err != nil {
		return fmt.Errorf("failed to create symlink %s: %v", newName, err)
	}

	return nil
}

// ReadDir return the contents of a given directory in a given filesystem.
//
// Returns a slice of os.FileInfo with all the entries in the directory.
//
// Will return an error if the directory does not exist or is a regular file and not a directory
func (fs *FileSystem) ReadDir(p string) ([]os.FileInfo, error) {
	dir, err := fs.readDirWithMkdir(p, false, 0)
	if err != nil {
		return nil, fmt.Errorf("error reading directory %s: %v", p, err)
	}
	// once we have made it here, looping is done. We have found the final entry
	// we need to return all the file info
	entries := dir.entries.Entries()
	count := len(entries)
	ret := make([]os.FileInfo, count)
	for i, e := range entries {
		in, err := fs.readInode(e.inode)
		if err != nil {
			return nil, fmt.Errorf("could not read inode %d at position %d in directory: %v", e.inode, i, err)
		}
		info := &FileInfo{
			modTime: in.modifyTime,
			name:    e.filename,
			mode:    os.FileMode(in.permissionsGroup.toGroupInt() | in.permissionsOther.toOtherInt() | in.permissionsOwner.toOwnerInt()),
			size:    int64(in.size),
			isDir:   e.fileType == dirFileTypeDirectory,
		}
		switch e.fileType {
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
		ret[i] = info
	}

	return ret, nil
}

// OpenFile returns an io.ReadWriter from which you can read the contents of a file
// or write contents to the file
//
// accepts normal os.OpenFile flags
//
// returns an error if the file does not exist
func (fs *FileSystem) OpenFile(p string, flag int, perm os.FileMode) (filesystem.File, error) {
	filename := path.Base(p)
	dir := path.Dir(p)
	parentDir, entry, err := fs.getEntryAndParent(p)
	if err != nil {
		return nil, err
	}
	if entry != nil {
		switch entry.fileType {
		case dirFileTypeSymlink, dirFileTypeRegular:
		case dirFileTypeSocket:
			return nil, fmt.Errorf("cannot open socket %s as file", p)
		case dirFileTypeFifo:
			return nil, fmt.Errorf("cannot open fifo %s as file", p)
		case dirFileTypeCharacter:
			return nil, fmt.Errorf("cannot open character device %s as file", p)
		case dirFileTypeDirectory:
			return nil, fmt.Errorf("cannot open directory %s as file", p)
		case dirFileTypeBlock:
			return nil, fmt.Errorf("cannot open block device %s as file", p)
		default:
			return nil, fmt.Errorf("cannot open unknown file type %d %s as file", entry.fileType, p)
		}
	}

	// see if the file exists
	// if the file does not exist, and is not opened for os.O_CREATE, return an error
	if entry == nil {
		if flag&os.O_CREATE == 0 {
			return nil, fmt.Errorf("target file %s does not exist and was not asked to create", p)
		}
		// else create it
		entry, err = fs.mkFile(parentDir, filename, perm)
		if err != nil {
			return nil, fmt.Errorf("failed to create file %s: %v", p, err)
		}
	}
	// get the inode
	inodeNumber := entry.inode
	inode, err := fs.readInode(inodeNumber)
	if err != nil {
		return nil, fmt.Errorf("could not read inode number %d: %v", inodeNumber, err)
	}

	// if a symlink, read the target, rather than the inode itself, which does not point to anything
	if inode.fileType == fileTypeSymbolicLink {
		// is the symlink relative or absolute?
		linkTarget := inode.linkTarget
		if !path.IsAbs(linkTarget) {
			// convert it into an absolute path
			// and start the process again
			linkTarget = path.Join(dir, linkTarget)
			// we probably could make this more efficient by checking if the final linkTarget
			// is in the same directory as we already are parsing, rather than walking the whole thing again
			// leave that for the future.
			linkTarget = path.Clean(linkTarget)
		}
		return fs.OpenFile(linkTarget, flag, perm)
	}
	offset := int64(0)
	if flag&os.O_APPEND == os.O_APPEND {
		offset = int64(inode.size)
	}
	// when we open a file, we load the inode but also all of the extents
	extents, err := inode.extents.blocks(fs)
	if err != nil {
		return nil, fmt.Errorf("could not read extent tree for inode %d: %v", inodeNumber, err)
	}
	return &File{
		directoryEntry: entry,
		inode:          inode,
		isWriteOnly:    flag&os.O_WRONLY != 0,
		isReadWrite:    flag&os.O_RDWR != 0,
		isAppend:       flag&os.O_APPEND != 0,
		offsetWrite:    offset,
		filesystem:     fs,
		extents:        extents,
	}, nil
}

// Rename changes the name and/or location of the file
func (fs *FileSystem) Rename(from, to string) error {
	from = path.Clean(from)
	to = path.Clean(to)
	filenameTo := path.Base(to)
	dirFrom := path.Dir(from)
	dirTo := path.Dir(to)

	parentDirFrom, entry, err := fs.getEntryAndParent(from)
	if err != nil {
		return err
	}

	parentDirTo, entryTo, err := fs.getEntryAndParent(to)
	if err != nil {
		return err
	}

	if entry == nil {
		return fmt.Errorf("%w: %s", os.ErrNotExist, from)
	}
	if entryTo != nil {
		return fmt.Errorf("%w: %s", os.ErrExist, to)
	}

	// change the entry information
	entry.filename = filenameTo
	entry.fileNameLen = uint16(len(filenameTo))
	entry.length = uint16(entry.CalcSize())

	// check if parent has increased in size beyond allocated blocks
	parentInodeTo, err := fs.readInode(parentDirTo.inode)
	if err != nil {
		return fmt.Errorf("could not read inode %d of parent directory: %w", parentDirTo.inode, err)
	}
	parentInodeFrom, err := fs.readInode(parentDirFrom.inode)
	if err != nil {
		return fmt.Errorf("could not read inode %d of parent directory: %w", parentDirFrom.inode, err)
	}

	// did not change directories
	if dirTo == dirFrom {
		return fs.writeDir(parentDirFrom, parentInodeFrom)
	}

	parentBlocksTo := blocksRequired(parentDirTo.entries.Size(), fs.superblock.blockSize)
	parentDirTo.entries.AddEntry(entry)
	parentDirFrom.entries.RemoveEntry(entry)

	parentEntriesSizeTo := parentDirTo.entries.Size()
	if required := blocksRequired(parentEntriesSizeTo, fs.superblock.blockSize); required > parentBlocksTo {
		var (
			parentExtentsExisting extents
			parentExtendsNew      *extents
		)
		if parentExtentsExisting, err = parentInodeTo.extents.blocks(fs); err != nil {
			return err
		}
		// allocateExtends removes existing extents blocks from the bytes needed internally
		if parentExtendsNew, err = fs.allocateExtents(uint64(required*fs.superblock.blockSize), &parentExtentsExisting); err != nil {
			return fmt.Errorf("could not allocate disk space for file %w", err)
		}
		if parentInodeTo.extents, err = extendExtentTree(parentInodeTo.extents, parentExtendsNew, fs, nil); err != nil {
			return fmt.Errorf("could not convert extents into tree: %w", err)
		}
	}

	if entry.fileType == dirFileTypeDirectory {
		parentInodeTo.hardLinks++
		if err = fs.writeInode(parentInodeTo); err != nil {
			return fmt.Errorf("could not write inode: %w", err)
		}
		parentInodeFrom.hardLinks--
		if err = fs.writeInode(parentInodeFrom); err != nil {
			return fmt.Errorf("could not write inode: %w", err)
		}
	}

	// write the parent out to disk
	if err = fs.writeDir(parentDirTo, parentInodeTo); err != nil {
		return err
	}
	return fs.writeDir(parentDirFrom, parentInodeFrom)
}

func (fs *FileSystem) writeDir(dir *Directory, in *inode) (err error) {
	bs := make([]byte, dir.entries.Size())
	if err = dir.entries.MarshalExt4(bs); err != nil {
		return fmt.Errorf("failed to marshal parent directory: %w", err)
	}
	parentExtentsFrom, err := in.extents.blocks(fs)
	if err != nil {
		return fmt.Errorf("could not read parent extents for directory: %w", err)
	}
	dirFileFrom := &File{
		inode:          in,
		directoryEntry: &dir.directoryEntry,
		filesystem:     fs,
		isReadWrite:    true,
		isAppend:       true,
		extents:        parentExtentsFrom,
	}
	var wrote int
	if wrote, err = dirFileFrom.Write(bs); err != nil && err != io.EOF {
		return fmt.Errorf("unable to write new directory: %w", err)
	} else if wrote != len(bs) {
		return fmt.Errorf("wrote only %d bytes instead of expected %d for new directory", wrote, len(bs))
	}
	return nil
}

// Label read the volume label
func (fs *FileSystem) Label() string {
	if fs.superblock == nil {
		return ""
	}
	return fs.superblock.volumeLabel
}

// Remove file or directory at path.
// If path is directory, it only will remove if it is empty.
// If path is a file, it will remove the file.
// Will not remove any parents.
// Error if the file does not exist or is not an empty directory
func (fs *FileSystem) Remove(p string) error {
	parentDir, entry, err := fs.getEntryAndParent(p)
	if err != nil {
		return err
	}
	if parentDir.root && entry == &parentDir.directoryEntry {
		return fmt.Errorf("cannot remove root directory")
	}
	if entry == nil {
		return fmt.Errorf("%w: %s", os.ErrNotExist, p)
	}
	// if it is a directory, it must be empty
	if entry.fileType == dirFileTypeDirectory {
		// read the directory
		entries, err := fs.readDirectory(entry.inode)
		if err != nil {
			return fmt.Errorf("could not read directory %s: %v", p, err)
		}
		if len(entries.Entries()) > 2 {
			return fmt.Errorf("directory not empty: %s", p)
		}
	}
	// at this point, it is either a file or an empty directory, so remove it

	// free up the blocks
	// read the inode to find the blocks
	removedInode, err := fs.readInode(entry.inode)
	if err != nil {
		return fmt.Errorf("could not read inode %d for %s: %v", entry.inode, p, err)
	}

	// clear the inode from the inode bitmap
	inodeBG := blockGroupForInode(entry.inode, fs.superblock.inodesPerGroup)
	inodeBitmap, err := fs.readInodeBitmap(inodeBG)
	if err != nil {
		return fmt.Errorf("could not read inode bitmap: %v", err)
	}

	var exts extents
	if removedInode.extents != nil {
		if exts, err = removedInode.extents.blocks(fs); err != nil {
			return fmt.Errorf("could not read extents for inode %d for %s: %v", entry.inode, p, err)
		}

		// clear up the blocks from the block bitmap. We are not clearing the block content, just the bitmap.
		// keep a cache of bitmaps, so we do not have to read them again and again
		blockBitmaps := make(map[int]*util.Bitmap)
		for _, e := range exts {
			for i := e.startingBlock; i < e.startingBlock+uint64(e.count); i++ {
				// determine what block group this block is in, and read the bitmap for that blockgroup
				bg := blockGroupForBlock(int(i), fs.superblock.blocksPerGroup)
				dataBlockBitmap, ok := blockBitmaps[bg]
				if !ok {
					dataBlockBitmap, err = fs.readBlockBitmap(bg)
					if err != nil {
						return fmt.Errorf("could not read block bitmap: %v", err)
					}
					blockBitmaps[bg] = dataBlockBitmap
				}
				// the extent lists the absolute block number, but the bitmap is relative to the block group
				blockInBG := int(i) - int(fs.superblock.blocksPerGroup)*bg
				if err = dataBlockBitmap.Clear(blockInBG); err != nil {
					return fmt.Errorf("could not clear block bitmap for block %d: %v", i, err)
				}
			}
		}
		for bg, dataBlockBitmap := range blockBitmaps {
			if err = fs.writeBlockBitmap(dataBlockBitmap, bg); err != nil {
				return fmt.Errorf("could not write block bitmap back to disk: %v", err)
			}
		}
	}

	// remove the directory entry from the parent
	parentDir.entries.RemoveEntry(entry)

	parentInode, err := fs.readInode(parentDir.inode)
	if err != nil {
		return fmt.Errorf("could not read inode %d for %s: %v", entry.inode, path.Base(p), err)
	}

	// write the parent directory back
	if err = fs.writeDir(parentDir, parentInode); err != nil {
		return err
	}

	// remove the hardlink from the inode
	removedInode.hardLinks--
	if err = fs.writeInode(removedInode); err != nil {
		return fmt.Errorf("failed to write removed inode: %w", err)
	}

	// remove the inode from the bitmap and write the inode bitmap back
	// inode is absolute, but bitmap is relative to block group
	inodeInBG := int(entry.inode) - int(fs.superblock.inodesPerGroup)*inodeBG
	if err = inodeBitmap.Clear(inodeInBG - 1); err != nil {
		return fmt.Errorf("could not clear inode bitmap for inode %d: %v", entry.inode, err)
	}

	// write the inode bitmap back
	if err = fs.writeInodeBitmap(inodeBitmap, inodeBG); err != nil {
		return fmt.Errorf("could not write inode bitmap back to disk: %v", err)
	}
	// update the group descriptor
	gd := fs.groupDescriptors.descriptors[inodeBG]

	// update the group descriptor inodes and blocks
	gd.freeInodes++
	gd.freeBlocks += uint32(removedInode.blocks)
	// write the group descriptor back
	if err = fs.writeGroupDescriptor(gd); err != nil {
		return err
	}

	// we could remove the inode from the inode table in the group descriptor,
	// but we do not need to do so. Since we are not reusing the inode, we can just leave it there,
	// the bitmap always is checked before reusing an inode location.
	fs.superblock.freeInodes++
	fs.superblock.freeBlocks += removedInode.blocks
	return fs.writeSuperblock()
}

func (fs *FileSystem) writeGroupDescriptor(gd *groupDescriptor) (err error) {
	gdBytes := gd.toBytes(fs.superblock.gdtChecksumType(), fs.getCheckSumSeed(fs.superblock))
	gdtBlock := getGDTBlock(fs.superblock)
	gdOffset := fs.start + gdtBlock*int64(fs.superblock.blockSize) + int64(gd.number)*int64(fs.superblock.groupDescriptorSize)
	if _, err = fs.file.WriteAt(gdBytes, gdOffset); err != nil {
		return fmt.Errorf("could not write Group Descriptor bytes to file: %v", err)
	}
	return err
}

func (fs *FileSystem) Truncate(p string, size int64) error {
	_, entry, err := fs.getEntryAndParent(p)
	if err != nil {
		return err
	}
	if entry == nil {
		return fmt.Errorf("%w: %s", os.ErrNotExist, p)
	}
	if entry.fileType == dirFileTypeDirectory {
		return fmt.Errorf("cannot truncate directory %s", p)
	}
	// it is not a directory, and it exists, so truncate it
	inode, err := fs.readInode(entry.inode)
	if err != nil {
		return fmt.Errorf("could not read inode %d in directory: %v", entry.inode, err)
	}
	// change the file size
	inode.size = uint64(size)

	// free used blocks if shrank, or reserve new blocks if grew
	// both of which mean updating the superblock, and the extents tree in the inode

	// write the inode back
	return fs.writeInode(inode)
}

// getEntryAndParent given a path, get the Directory for the parent and the directory entry for the file.
// If the directory does not exist, returns an error.
// If the file does not exist, does not return an error, but rather returns a nil entry.
func (fs *FileSystem) getEntryAndParent(p string) (parent *Directory, entry *directoryEntry, err error) {
	dir := path.Dir(p)
	filename := path.Base(p)
	// get the directory entries
	parentDir, err := fs.readDirWithMkdir(dir, false, 0)
	if err != nil {
		return nil, nil, fmt.Errorf("could not read directory entries for %s: %w", dir, err)
	}
	// we now know that the directory exists, see if the file exists
	var targetEntry *directoryEntry
	if parentDir.root && filename == "/" {
		// root directory
		return parentDir, &parentDir.directoryEntry, nil
	}

	for _, e := range parentDir.entries.Entries() {
		if e.filename != filename {
			continue
		}
		// if we got this far, we have found the file
		targetEntry = e
		break
	}
	return parentDir, targetEntry, nil
}

// Stat return fs.FileInfo about a specific file path.
func (fs *FileSystem) Stat(p string) (os.FileInfo, error) {
	_, entry, err := fs.getEntryAndParent(p)
	if err != nil {
		return nil, err
	}
	if entry == nil {
		return nil, fmt.Errorf("%w: %s", os.ErrNotExist, p)
	}
	in, err := fs.readInode(entry.inode)
	if err != nil {
		return nil, fmt.Errorf("could not read inode %d in directory: %v", entry.inode, err)
	}
	info := &FileInfo{
		modTime: in.modifyTime,
		name:    entry.filename,
		size:    int64(in.size),
		isDir:   entry.fileType == dirFileTypeDirectory,
	}
	switch entry.fileType {
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

// SetLabel changes the label on the writable filesystem. Different file system may hav different
// length constraints.
func (fs *FileSystem) SetLabel(label string) error {
	fs.superblock.volumeLabel = label
	return fs.writeSuperblock()
}

// readInode read a single inode from disk
func (fs *FileSystem) readInode(inodeNumber uint32) (*inode, error) {
	if inodeNumber == 0 {
		return nil, fmt.Errorf("cannot read inode 0")
	}
	sb := fs.superblock

	// figure out which block group the inode is on
	bg := blockGroupForInode(inodeNumber, sb.inodesPerGroup)
	inodeBytes := make([]byte, sb.inodeSize)
	read, err := fs.file.ReadAt(inodeBytes, fs.inodeByteLocation(fs.groupDescriptors.descriptors[bg], inodeNumber))
	if err != nil {
		return nil, fmt.Errorf("failed to read inode %d from block group %d: %v", inodeNumber, bg, err)
	}
	if read != int(sb.inodeSize) {
		return nil, fmt.Errorf("read %d bytes for inode %d instead of inode size of %d", read, inodeNumber, sb.inodeSize)
	}
	iNode, err := inodeFromBytes(inodeBytes, sb, inodeNumber)
	if err != nil {
		return nil, fmt.Errorf("could not interpret inode data: %v", err)
	}

	// fill in symlink target if needed
	if iNode.fileType == fileTypeSymbolicLink && iNode.linkTarget == "" {
		// read the symlink target
		var iExtents extents
		if iExtents, err = iNode.extents.blocks(fs); err != nil {
			return nil, fmt.Errorf("could not read extent tree for symlink inode %d: %v", inodeNumber, err)
		}
		var b []byte
		if b, err = fs.readFileBytes(iExtents, iNode.size); err != nil {
			return nil, fmt.Errorf("could not read symlink target for inode %d: %v", inodeNumber, err)
		}
		iNode.linkTarget = string(b)
	}
	return iNode, nil
}

// writeInode write a single inode to disk
func (fs *FileSystem) writeInode(i *inode) error {
	sb := fs.superblock
	// figure out which block group the inode is on
	bg := blockGroupForInode(i.number, sb.inodesPerGroup)
	inodeBytes := i.toBytes(sb)
	wrote, err := fs.file.WriteAt(inodeBytes, fs.inodeByteLocation(fs.groupDescriptors.descriptors[bg], i.number))
	if err != nil {
		return fmt.Errorf("failed to write inode %d from block group %d: %v", i.number, bg, err)
	}
	if wrote != int(sb.inodeSize) {
		return fmt.Errorf("wrote %d bytes for inode %d instead of inode size of %d", wrote, i.number, sb.inodeSize)
	}
	return nil
}

// read directory entries for a given directory
func (fs *FileSystem) readDirectory(inodeNumber uint32) (entries directoryEntries, err error) {
	// read the inode for the directory
	in, err := fs.readInode(inodeNumber)
	if err != nil {
		return nil, fmt.Errorf("could not read inode %d for directory: %v", inodeNumber, err)
	}
	// convert the extent tree into a sorted list of extents
	extentBlocks, err := in.extents.blocks(fs)
	if err != nil {
		return nil, fmt.Errorf("unable to get blocks for inode %d: %w", in.number, err)
	}
	// read the contents of the file across all blocks
	b, err := fs.readFileBytes(extentBlocks, in.size)
	if err != nil {
		return nil, fmt.Errorf("error reading file bytes for inode %d: %v", inodeNumber, err)
	}

	if in.flags.hashedDirectoryIndexes {
		hash := &directoryEntriesHashTreeRoot{
			large: fs.superblock.features.largeDirectory,
			directoryHashNode: directoryHashNode{
				hashTreeSeed:  fs.superblock.hashTreeSeed,
				hasFileType:   fs.superblock.features.directoryEntriesRecordFileType,
				bytesPerBlock: fs.superblock.blockSize,
				major:         true,
			},
		}
		if fs.superblock.features.metadataChecksums {
			hash.directoryHashNode.linearCheckSum = linearDirectoryCheckSum(fs.superblock.checksumSeed, in.number, in.nfsFileVersion)
			hash.directoryHashNode.hashTreeCheckSum = hashTreeDirectoryCheckSum(fs.superblock.checksumSeed, in.number, in.nfsFileVersion)
		}
		entries = hash
	} else {
		// convert into directory entries
		linear := &directoryEntriesLinear{
			bytesPerBlock: fs.superblock.blockSize,
			hasFileType:   fs.superblock.features.directoryEntriesRecordFileType,
		}
		if fs.superblock.features.metadataChecksums {
			linear.checkSum = linearDirectoryCheckSum(fs.superblock.checksumSeed, in.number, in.nfsFileVersion)
		}
		entries = linear
	}
	if err = entries.UnmarshalExt4(b); err != nil {
		return nil, fmt.Errorf("error unmarshaling directory entries: %w", err)
	}
	return entries, err
}

// readFileBytes read all the bytes for an individual file pointed at by a given inode
// normally not very useful, but helpful when reading an entire directory.
func (fs *FileSystem) readFileBytes(extents extents, filesize uint64) (b []byte, err error) {
	// walk through each one, gobbling up the bytes

	b = make([]byte, filesize)

	var (
		index uint64
		next  uint64
		read  int
		start uint64
		count uint64
	)
	for i, e := range extents {
		start = e.startingBlock * uint64(fs.superblock.blockSize)
		count = uint64(e.count) * uint64(fs.superblock.blockSize)
		if next = index + count; next > filesize {
			next = filesize
		}
		if read, err = fs.file.ReadAt(b[index:next], int64(start)+fs.start); err != nil {
			return nil, fmt.Errorf("failed to read bytes for extent %d: %v", i, err)
		}
		if read != int(count) {
			return nil, fmt.Errorf("read %d bytes instead of %d for extent %d", read, count, i)
		}
		if index = next; index >= filesize {
			break
		}
	}
	return b, nil
}

// mkFile make a file with a given name in the given directory.
func (fs *FileSystem) mkFile(parent *Directory, name string, perm os.FileMode) (*directoryEntry, error) {
	return fs.mkDirEntry(parent, name, "", perm & ^os.ModeDir)
}

// readDirWithMkdir - walks down a directory tree to the last entry in p.
// For example, if p is /a/b/c, it will walk down to c.
// Expects c to be a directory.
// If each step in the tree does not exist, it will either make it if doMake is true, or return an error.
func (fs *FileSystem) readDirWithMkdir(p string, doMake bool, perm os.FileMode) (currentDir *Directory, err error) {
	var (
		entry       *directoryEntry
		subdirEntry *directoryEntry
		node        *inode
		found       bool
	)
	paths := splitPath(p)

	// walk down the directory tree until all paths have been walked or we cannot find something
	// start with the root directory
	currentDir = &Directory{
		directoryEntry: directoryEntry{
			inode:    rootInode,
			filename: "",
			fileType: dirFileTypeDirectory,
		},
		root: true,
	}
	if currentDir.entries, err = fs.readDirectory(rootInode); err != nil {
		return nil, fmt.Errorf("failed to read directory %s: %w", "/", err)
	}
	for i, subPath := range paths {
		// do we have an entry whose name is the same as this name?
		// TODO: use hash tree performance gains but calculating hashes and doing direct lookups instead of walking the entire tree
		found = false
		for _, entry = range currentDir.entries.Entries() {
			if entry.filename != subPath {
				continue
			}
			switch entry.fileType {
			case dirFileTypeSymlink:
				// if symlink find target and follow
				if node, err = fs.readInode(entry.inode); err != nil {
					return nil, fmt.Errorf("failed to read inode at symbolic link: %w", err)
				}
				if currentDir, err = fs.readDirWithMkdir(node.linkTarget, doMake, perm); err != nil {
					return nil, err
				}
			case dirFileTypeDirectory:
				// the filename matches, and it is a subdirectory, so we can break after saving the directory entry, which contains the inode
				currentDir = &Directory{
					directoryEntry: *entry,
				}
			default:
				return nil, fmt.Errorf("cannot create directory at %s since it is a file", "/"+strings.Join(paths[0:i+1], "/"))
			}
			found = true
			break
		}

		// if not, either make it, retrieve its cluster and entries, and loop;
		//  or error out
		if !found {
			if doMake {
				if subdirEntry, err = fs.mkSubDir(currentDir, subPath, perm); err != nil {
					return nil, fmt.Errorf("failed to create subdirectory %s: %w", "/"+strings.Join(paths[0:i+1], "/"), err)
				}
				// save where we are to search next
				currentDir = &Directory{
					directoryEntry: *subdirEntry,
				}
			} else {
				return nil, fmt.Errorf("path %s not found", "/"+strings.Join(paths[0:i+1], "/"))
			}
		}
		// get all the entries in this directory
		if currentDir.entries, err = fs.readDirectory(currentDir.inode); err != nil {
			return nil, fmt.Errorf("failed to read directory %s: %w", "/"+strings.Join(paths[0:i+1], "/"), err)
		}
	}
	// once we have made it here, looping is done; we have found the final entry
	return currentDir, nil
}

// readBlock read a single block from disk
func (fs *FileSystem) readBlock(blockNumber uint64, b []byte) error {
	sb := fs.superblock
	// bytesStart is beginning byte for the inodeTableBlock
	byteStart := blockNumber * uint64(sb.blockSize)
	read, err := fs.file.ReadAt(b, int64(byteStart)+fs.start)
	if err != nil {
		return fmt.Errorf("failed to read block %d: %v", blockNumber, err)
	}
	if read != int(sb.blockSize) {
		return fmt.Errorf("read %d bytes for block %d instead of size of %d", read, blockNumber, sb.blockSize)
	}
	return nil
}

// recalculate blocksize based on the existing number of blocks
// -      0 <= blocks <   3MM         : floppy - blocksize = 1024
// -    3MM <= blocks < 512MM         : small - blocksize = 1024
// - 512MM <= blocks < 4*1024*1024MM  : default - blocksize =
// - 4*1024*1024MM <= blocks < 16*1024*1024MM  : big - blocksize =
// - 16*1024*1024MM <= blocks   : huge - blocksize =
//
// the original code from e2fsprogs https://git.kernel.org/pub/scm/fs/ext2/e2fsprogs.git/tree/misc/mke2fs.c
func recalculateBlockSize(numBlocks, size int64) (sectorsPerBlock int, blockSize uint32, numBlocksAdjusted int64) {
	var (
		million64     = int64(million)
		sectorSize512 = uint32(SectorSize512)
	)
	switch {
	case 0 <= numBlocks && numBlocks < 3*million64:
		sectorsPerBlock = 2
		blockSize = 2 * sectorSize512
	case 3*million64 <= numBlocks && numBlocks < 512*million64:
		sectorsPerBlock = 2
		blockSize = 2 * sectorSize512
	case 512*million64 <= numBlocks && numBlocks < 4*1024*1024*million64:
		sectorsPerBlock = 2
		blockSize = 2 * sectorSize512
	case 4*1024*1024*million64 <= numBlocks && numBlocks < 16*1024*1024*million64:
		sectorsPerBlock = 2
		blockSize = 2 * sectorSize512
	case numBlocks > 16*1024*1024*million64:
		sectorsPerBlock = 2
		blockSize = 2 * sectorSize512
	}
	return sectorsPerBlock, blockSize, size / int64(blockSize)
}

// mkSubDir make a subdirectory of a given name inside the parent
// 1- allocate a single data block for the directory
// 2- create an inode in the inode table pointing to that data block
// 3- mark the inode in the inode bitmap
// 4- mark the data block in the data block bitmap
// 5- create a directory entry in the parent directory data blocks
func (fs *FileSystem) mkSubDir(parent *Directory, name string, perm os.FileMode) (*directoryEntry, error) {
	return fs.mkDirEntry(parent, name, "", perm|os.ModeDir)
}

func (fs *FileSystem) mkDirEntry(parent *Directory, name string, linkTarget string, perm os.FileMode) (de *directoryEntry, err error) {
	// still to do:
	//  - write directory entry in parent
	//  - write inode to disk

	var (
		fType            fileType
		size             uint64
		contentSize      uint64
		hardLinks        uint16
		blockCount       uint64
		newExtents       *extents
		extentTreeParsed extentBlockFinder
		flags            inodeFlags
	)

	de = &directoryEntry{
		filename:    name,
		fileNameLen: uint16(len(name)),
		hasFileType: fs.superblock.features.directoryEntriesRecordFileType,
	}

	switch {
	case perm&os.ModeDir == os.ModeDir:
		de.fileType = dirFileTypeDirectory
		fType = fileTypeDirectory
		size = 1
		hardLinks = 2
		linkTarget = ""
		contentSize = uint64(fs.superblock.blockSize)
	case perm&os.ModeDevice == os.ModeDevice:
		de.fileType = dirFileTypeBlock
		fType = fileTypeBlockDevice
		size = 1
		linkTarget = ""
		return nil, errors.New("not supported")
	case perm&os.ModeCharDevice == os.ModeCharDevice:
		de.fileType = dirFileTypeCharacter
		fType = fileTypeCharacterDevice
		size = 1
		linkTarget = ""
		return nil, errors.New("not supported")
	case perm&os.ModeNamedPipe == os.ModeNamedPipe:
		de.fileType = dirFileTypeFifo
		fType = fileTypeFifo
		size = 1
		linkTarget = ""
		return nil, errors.New("not supported")
	case perm&os.ModeSocket == os.ModeSocket:
		de.fileType = dirFileTypeSocket
		fType = fileTypeSocket
		size = 1
		linkTarget = ""
		return nil, errors.New("not supported")
	case perm&os.ModeSymlink == os.ModeSymlink:
		de.fileType = dirFileTypeSymlink
		fType = fileTypeSymbolicLink
		hardLinks = 1
		contentSize = uint64(len(linkTarget))
		switch l := len(linkTarget); {
		case l > 0 && l <= 60:
			size = 0
		case l > 60:
			size = 1
		default:
			return nil, errors.New("missing target for symbolic link")
		}
	default:
		de.fileType = dirFileTypeRegular
		fType = fileTypeRegularFile
		size = 1
		hardLinks = 1
		linkTarget = ""
	}

	// create an inode
	if de.inode, err = fs.allocateInode(parent.inode, fType); err != nil {
		return nil, fmt.Errorf("could not allocate inode for file %s: %w", name, err)
	}
	// get extents for the file - prefer in the same block group as the inode, if possible
	if newExtents, err = fs.allocateExtents(size, nil); err != nil {
		return nil, fmt.Errorf("could not allocate disk space for file %s: %w", name, err)
	}
	if newExtents != nil {
		flags.usesExtents = true
		blockCount = newExtents.blockCount()
		if extentTreeParsed, err = extendExtentTree(nil, newExtents, fs, nil); err != nil {
			return nil, fmt.Errorf("could not convert extents into tree: %w", err)
		}
	}

	// normally, after getting a tree from extents, you would need to then allocate all the blocks
	//    in the extent tree - leafs and intermediate. However, because we are allocating a new directory
	//    with a single extent, we *know* it can fit in the inode itself (which has a max of 4), so no need

	de.length = uint16(de.CalcSize())

	// check if parent has increased in size beyond allocated blocks
	parentInode, err := fs.readInode(parent.inode)
	if err != nil {
		return nil, fmt.Errorf("could not read inode %d of parent directory: %w", parent.inode, err)
	}

	parentBlocks := blocksRequired(parent.entries.Size(), fs.superblock.blockSize)
	parent.entries.AddEntry(de)

	parentEntriesSize := parent.entries.Size()
	if required := blocksRequired(parentEntriesSize, fs.superblock.blockSize); required > parentBlocks {
		var (
			parentExtentsExisting extents
			parentExtendsNew      *extents
		)
		if parentExtentsExisting, err = parentInode.extents.blocks(fs); err != nil {
			return nil, err
		}
		// allocateExtends removes existing extents blocks from the bytes needed internally
		if parentExtendsNew, err = fs.allocateExtents(uint64(required*fs.superblock.blockSize), &parentExtentsExisting); err != nil {
			return nil, fmt.Errorf("could not allocate disk space for file %w", err)
		}
		if parentInode.extents, err = extendExtentTree(parentInode.extents, parentExtendsNew, fs, nil); err != nil {
			return nil, fmt.Errorf("could not convert extents into tree: %w", err)
		}
	}

	// write the parent out to disk
	parentDirBytes := make([]byte, parentEntriesSize)
	if err = parent.entries.MarshalExt4(parentDirBytes); err != nil {
		return nil, fmt.Errorf("failed to marshal parent directory: %w", err)
	}

	// write the directory entry in the parent
	// figure out which block it goes into, and possibly balance the directory entries hash tree
	parentExtents, err := parentInode.extents.blocks(fs)
	if err != nil {
		return nil, fmt.Errorf("could not read parent extents for directory: %w", err)
	}
	if fType == fileTypeDirectory {
		parentInode.hardLinks++
		if err = fs.writeInode(parentInode); err != nil {
			return nil, fmt.Errorf("could not write inode: %w", err)
		}
	}

	dirFile := &File{
		inode:          parentInode,
		directoryEntry: &parent.directoryEntry,
		filesystem:     fs,
		isReadWrite:    true,
		isAppend:       true,
		extents:        parentExtents,
	}
	wrote, err := dirFile.Write(parentDirBytes)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("unable to write new directory: %w", err)
	}
	if wrote != len(parentDirBytes) {
		return nil, fmt.Errorf("wrote only %d bytes instead of expected %d for new directory", wrote, len(parentDirBytes))
	}

	// write the inode for the new entry out
	now := time.Now()
	in := &inode{
		number:                 de.inode,
		fileType:               fType,
		owner:                  parentInode.owner,
		group:                  parentInode.group,
		size:                   contentSize,
		hardLinks:              hardLinks,
		blocks:                 blockCount,
		flags:                  &flags,
		nfsFileVersion:         0,
		version:                0,
		inodeSize:              fs.superblock.inodeSize,
		deletionTime:           0,
		accessTime:             now,
		changeTime:             now,
		createTime:             now,
		modifyTime:             now,
		extendedAttributeBlock: 0,
		project:                0,
		extents:                extentTreeParsed,
		linkTarget:             linkTarget,
	}

	in.permissionsOwner.read = perm&syscall.S_IRUSR == syscall.S_IRUSR
	in.permissionsOwner.write = perm&syscall.S_IWUSR == syscall.S_IWUSR
	in.permissionsOwner.execute = perm&syscall.S_IXUSR == syscall.S_IXUSR
	in.permissionsGroup.read = perm&syscall.S_IRGRP == syscall.S_IRGRP
	in.permissionsGroup.write = perm&syscall.S_IWGRP == syscall.S_IWGRP
	in.permissionsGroup.execute = perm&syscall.S_IXGRP == syscall.S_IXGRP
	in.permissionsOther.read = perm&syscall.S_IROTH == syscall.S_IROTH
	in.permissionsOther.write = perm&syscall.S_IWOTH == syscall.S_IWOTH
	in.permissionsOther.execute = perm&syscall.S_IXOTH == syscall.S_IXOTH
	in.sticky = perm&syscall.S_ISVTX == syscall.S_ISVTX
	in.setGID = perm&syscall.S_ISGID == syscall.S_ISGID
	in.setUID = perm&syscall.S_ISUID == syscall.S_ISUID

	// write the inode to disk
	if err = fs.writeInode(in); err != nil {
		return nil, fmt.Errorf("could not write inode for new directory: %w", err)
	}
	// if a directory, put entries for . and .. in the first block for the new directory
	if fType == fileTypeDirectory {
		// TODO: always is linear when making the directory, only move to Hash Tree when the linear entries exceeds a single block
		// TODO: in that case move the directory entries into two new equally distributed blocks (plus the hash tree root)
		// TODO: if the hash tree root's size exceeds a single block, increase the depth and rebuild

		linear := &directoryEntriesLinear{
			entries: []*directoryEntry{
				{
					inode:       de.inode,
					filename:    ".",
					fileType:    dirFileTypeDirectory,
					fileNameLen: 1,
					hasFileType: fs.superblock.features.directoryEntriesRecordFileType,
				},
				{
					inode:       parent.inode,
					filename:    "..",
					fileNameLen: 2,
					fileType:    dirFileTypeDirectory,
					hasFileType: fs.superblock.features.directoryEntriesRecordFileType,
				},
			},
			bytesPerBlock: fs.superblock.blockSize,
			hasFileType:   fs.superblock.features.directoryEntriesRecordFileType,
		}
		for _, entry := range linear.entries {
			entry.length = uint16(entry.CalcSize())
		}
		if fs.superblock.features.metadataChecksums {
			linear.checkSum = linearDirectoryCheckSum(fs.superblock.checksumSeed, de.inode, 0)
		}
		newDir := &Directory{
			directoryEntry: *de,
			root:           false,
			entries:        linear,
		}
		if err = fs.writeDir(newDir, in); err != nil {
			return nil, err
		}
	}

	// return
	return de, nil
}

// allocateInode allocate a single inode
// passed the parent, so it can know where to allocate it
// logic:
//   - parent is  0 : root inode, will allocate at 2
//   - parent is  2 : child of root, will try to spread out
//   - else         : try to collocate with parent, if possible
func (fs *FileSystem) allocateInode(parent uint32, fType fileType) (inodeNum uint32, err error) {
	var (
		bitmapLocation = -1
	)
	if parent == 0 {
		bitmapLocation = 1
	}
	// load the inode bitmap
	var (
		gd *groupDescriptor
		bm *util.Bitmap
	)
	for _, gd = range fs.groupDescriptors.descriptors {
		if bitmapLocation != -1 {
			break
		}
		if gd.freeInodes == 0 {
			continue
		}
		if bm, err = fs.readInodeBitmap(int(gd.number)); err != nil {
			return 0, fmt.Errorf("could not read inode bitmap: %w", err)
		}
		// get first free inode
		if bitmapLocation = bm.FirstFree(0); bitmapLocation == -1 {
			continue
		}

		bs := make([]byte, fs.superblock.inodeSize)
		num := fs.inodeNumFromBitmap(gd, bitmapLocation)
		if _, err = fs.file.ReadAt(bs, fs.inodeByteLocation(gd, num)); err != nil {
			return 0, fmt.Errorf("failed to read inode: %w", err)
		}
		var in *inode
		if in, err = inodeFromBytes(bs, fs.superblock, num); err == nil && in.hardLinks != 0 {
			return 0, fmt.Errorf("inode: %d in use", in.number)
		}
		// set it as marked
		if err = bm.Set(bitmapLocation); err != nil {
			return 0, fmt.Errorf("could not set inode bitmap: %w", err)
		}
		// write the inode bitmap bytes
		if err = fs.writeInodeBitmap(bm, int(gd.number)); err != nil {
			return 0, fmt.Errorf("could not write inode bitmap: %w", err)
		}
		break
	}
	if bitmapLocation == -1 || gd == nil {
		return 0, errors.New("no free inodes available")
	}

	// reduce number of free inodes in that descriptor in the group descriptor table
	gd.freeInodes--

	// increase the number of used directories if allocating an inode for a directory
	if fType == fileTypeDirectory {
		gd.usedDirectories++
	}

	// get the group descriptor as bytes
	if err = fs.writeGroupDescriptor(gd); err != nil {
		return 0, err
	}

	// write updated superblock and GDT to disk
	fs.superblock.freeInodes--
	if err = fs.writeSuperblock(); err != nil {
		return 0, fmt.Errorf("could not write superblock: %w", err)
	}

	return uint32(gd.number)*fs.superblock.inodesPerGroup + uint32(bitmapLocation) + 1, nil
}

func printFile(fs *FileSystem, prefix string, from int64, length int) {
	b := make([]byte, length)

	n, err := fs.file.ReadAt(b, from)
	if err != nil {
		fmt.Printf("failed to read: %s\n", err)
	}
	fmt.Printf("%s:\n\t%02X\n", prefix, b[:n])
}

// allocateExtents allocate the data blocks in extents that are
// to be used for a file of a given size
// arguments are file size in bytes and existing extents
// if previous is nil, then we are not (re)sizing an existing file but creating a new one
// returns the extents to be used in order
func (fs *FileSystem) allocateExtents(size uint64, previous *extents) (*extents, error) {
	// 1- calculate how many blocks are needed
	required := size / uint64(fs.superblock.blockSize)
	if size%uint64(fs.superblock.blockSize) > 0 {
		required++
	}
	// 2- see how many blocks already are allocated
	var allocated uint64
	if previous != nil {
		allocated = previous.blockCount()
	}
	// 3- if needed, allocate new blocks in extents
	extraBlockCount := required - allocated
	// if we have enough, do not add anything
	if extraBlockCount <= 0 {
		return previous, nil
	}

	// if there are not enough blocks left on the filesystem, return an error
	if fs.superblock.freeBlocks < extraBlockCount {
		return nil, fmt.Errorf("only %d blocks free, requires additional %d", fs.superblock.freeBlocks, extraBlockCount)
	}

	// now we need to look for as many contiguous blocks as possible
	// first calculate the minimum number of extents needed

	// if all the extents, except possibly the last, are maximum size, then we need minExtents extents
	// we loop through, trying to allocate an extent as large as our remaining blocks or maxBlocksPerExtent,
	//   whichever is smaller
	blockGroupCount := fs.blockGroups
	// TODO: instead of starting with BG 0, should start with BG where the inode for this file/dir is located
	var (
		newExtents       []extent
		dataBlockBitmaps = map[int]*util.Bitmap{}
		usedBlocks       = make(map[int]int)
		blocksPerGroup   = fs.superblock.blocksPerGroup
	)

	var i int64

	var allocatedBlocks uint64

	// change extraBlockCount is derived from allocated and thus allocated < extraBlockCount is incorrect.
	// i.e. required = 2, allocated = 1 ~ extraBlockCount == 1
	//      then allocated < extraBlockCount == false when an extraBlock needs to be allocated
	for i = 0; i < blockGroupCount && extraBlockCount-allocatedBlocks > 0; i++ {
		// keep track if we allocated anything in this blockgroup
		// 1- read the GDT for this blockgroup to find the location of the block bitmap
		//    and total free blocks
		// 2- read the block bitmap from disk
		// 3- find the maximum contiguous space available
		bs, err := fs.readBlockBitmap(int(i))
		if err != nil {
			return nil, fmt.Errorf("could not read block bitmap for block group %d: %v", i, err)
		}
		// now find our unused blocks and how many there are in a row as potential extents
		if extraBlockCount > maxUint16 {
			return nil, fmt.Errorf("cannot allocate more than %d blocks in a single extent", maxUint16)
		}

		// get the list of free blocks
		blockList := bs.FreeList()

		// create possible extents by size
		// Step 3: Group contiguous blocks into extents
		var exts []extent
		for _, freeBlock := range blockList {
			start, length := freeBlock.Position, freeBlock.Count
			for length > 0 {
				extentLength := min(length, int(maxBlocksPerExtent))
				exts = append(exts, extent{startingBlock: uint64(start) + uint64(i)*uint64(blocksPerGroup), count: uint16(extentLength)})
				start += extentLength
				length -= extentLength
			}
		}

		// sort in descending order
		sort.Slice(exts, func(i, j int) bool {
			return exts[i].count > exts[j].count
		})

		// TODO: growing internal nodes are not supported yet, try to find three extents that can hold the data
		maxExtents := 4
		if previous != nil {
			maxExtents -= len(*previous)
		}
		if len(exts) > maxExtents {
			exts = exts[:maxExtents]
		}

		count := uint64(0)
		for _, ext := range exts {
			count += uint64(ext.count)
		}
		if count < extraBlockCount {
			continue
		}

		for _, ext := range exts {
			if extraBlockCount-allocatedBlocks <= 0 {
				break
			}
			extentToAdd := ext
			if uint64(ext.count) >= (extraBlockCount - allocatedBlocks) {
				extentToAdd = extent{startingBlock: ext.startingBlock, count: uint16(extraBlockCount - allocatedBlocks)}
			}
			newExtents = append(newExtents, extentToAdd)
			allocatedBlocks += uint64(extentToAdd.count)
			// set the marked blocks in the bitmap, and save the bitmap
			for block := extentToAdd.startingBlock; block < extentToAdd.startingBlock+uint64(extentToAdd.count); block++ {
				// determine what block group this block is in, and read the bitmap for that blockgroup
				// the extent lists the absolute block number, but the bitmap is relative to the block group
				blockInGroup := block - uint64(i)*uint64(blocksPerGroup)
				if err = bs.Set(int(blockInGroup)); err != nil {
					return nil, fmt.Errorf("could not clear block bitmap for block %d: %v", i, err)
				}
			}

			// do *not* write the bitmap back yet, as we do not yet know if we will be able to fulfill the entire request.
			// instead save it for later
			dataBlockBitmaps[int(i)] = bs
			usedBlocks[int(i)] += int(extentToAdd.count)
		}
	}
	if extraBlockCount-allocatedBlocks > 0 {
		return nil, fmt.Errorf("could not allocate %d blocks", extraBlockCount)
	}

	// update the fileBlock for the new extends
	current := uint32(0)
	if previous != nil {
		if prev := *previous; len(prev) > 0 {
			current = prev[len(prev)-1].fileBlock + uint32(prev[len(prev)-1].count)
		}
	}
	for j := range newExtents {
		newExtents[j].fileBlock = current
		current = newExtents[j].fileBlock + uint32(newExtents[j].count)
	}

	// write the block bitmaps back to disk
	for bg, bs := range dataBlockBitmaps {
		if err := fs.writeBlockBitmap(bs, bg); err != nil {
			return nil, fmt.Errorf("could not write block bitmap for block group %d: %v", bg, err)
		}

		gd := fs.groupDescriptors.descriptors[bg]
		gd.freeBlocks -= uint32(usedBlocks[bg])
		// write the group descriptor back
		if err := fs.writeGroupDescriptor(gd); err != nil {
			return nil, err
		}
	}

	// need to update the total blocks used/free in superblock
	// fix: only remove the number of new blocks created not fs.superblock.freeBlocks -= allocated
	fs.superblock.freeBlocks -= extraBlockCount
	// update the blockBitmapChecksum for any updated block groups in GDT
	// write updated superblock and GDT to disk
	if err := fs.writeSuperblock(); err != nil {
		return nil, fmt.Errorf("could not write superblock: %w", err)
	}

	// update the previous
	if previous != nil {
		*previous = append(*previous, newExtents...)
	}

	// write backup copies
	var exten extents = newExtents
	return &exten, nil
}

func (fs *FileSystem) getCheckSumSeed(sb *superblock) uint32 {
	if sb.features.metadataChecksums {
		return sb.checksumSeed
	} else if sb.features.gdtChecksum {
		return fs.superblock.uuid.ID()
	}
	return 0
}

// readInodeBitmap read the inode bitmap off the disk.
// This would be more efficient if we just read one group descriptor's bitmap
// but for now we are about functionality, not efficiency, so it will read the whole thing.
func (fs *FileSystem) readInodeBitmap(group int) (*util.Bitmap, error) {
	if group >= len(fs.groupDescriptors.descriptors) {
		return nil, fmt.Errorf("block group %d does not exist", group)
	}
	gd := fs.groupDescriptors.descriptors[group]
	bitmapByteCount := fs.superblock.inodesPerGroup / 8
	b := make([]byte, bitmapByteCount)
	read, err := fs.file.ReadAt(b, fs.inodeBitmapByteLocation(gd))
	if err != nil {
		return nil, fmt.Errorf("unable to read inode bitmap for blockgroup %d: %w", gd.number, err)
	}
	if read != int(bitmapByteCount) {
		return nil, fmt.Errorf("read %d bytes instead of expected %d for inode bitmap of block group %d", read, bitmapByteCount, gd.number)
	}
	// only take bytes corresponding to the number of inodes per group

	// create a bitmap
	bm := util.BitmapWithBytes(b)

	if fs.superblock.features.metadataChecksums {
		if calc := bitmapChecksum(b, fs.superblock.checksumSeed, fs.superblock.features.fs64Bit); gd.inodeBitmapChecksum != calc {
			return nil, fmt.Errorf("checksum's do no match: expected %d, received %d", gd.inodeBitmapChecksum, calc)
		}
	}

	return bm, nil
}

// writeInodeBitmap write the inode bitmap to the disk.
func (fs *FileSystem) writeInodeBitmap(bm *util.Bitmap, group int) error {
	if group >= len(fs.groupDescriptors.descriptors) {
		return fmt.Errorf("block group %d does not exist", group)
	}
	b := bm.ToBytes()
	gd := fs.groupDescriptors.descriptors[group]

	wrote, err := fs.file.WriteAt(b, fs.inodeBitmapByteLocation(gd))
	if err != nil {
		return fmt.Errorf("unable to write inode bitmap for blockgroup %d: %w", gd.number, err)
	}
	if bitmapByteCount := fs.superblock.inodesPerGroup / 8; wrote != int(bitmapByteCount) {
		return fmt.Errorf("wrote %d bytes instead of expected %d for inode bitmap of block group %d", wrote, bitmapByteCount, gd.number)
	}

	if fs.superblock.features.metadataChecksums {
		gd.inodeBitmapChecksum = bitmapChecksum(b, fs.superblock.checksumSeed, fs.superblock.features.fs64Bit)
		if err = fs.writeGroupDescriptor(gd); err != nil {
			return err
		}
	}

	return nil
}

func (fs *FileSystem) readBlockBitmap(group int) (*util.Bitmap, error) {
	if group >= len(fs.groupDescriptors.descriptors) {
		return nil, fmt.Errorf("block group %d does not exist", group)
	}
	gd := fs.groupDescriptors.descriptors[group]
	bitmapLocation := gd.blockBitmapLocation
	b := make([]byte, fs.superblock.blockSize)
	offset := int64(bitmapLocation*uint64(fs.superblock.blockSize)) + fs.start
	read, err := fs.file.ReadAt(b, offset)
	if err != nil {
		return nil, fmt.Errorf("unable to read block bitmap for blockgroup %d: %w", gd.number, err)
	}
	if read != int(fs.superblock.blockSize) {
		return nil, fmt.Errorf("read %d bytes instead of expected %d for block bitmap of block group %d", read, fs.superblock.blockSize, gd.number)
	}
	// create a bitmap
	bs := util.NewBitmap(int(fs.superblock.blockSize) * len(fs.groupDescriptors.descriptors))
	bs.FromBytes(b)

	if fs.superblock.features.metadataChecksums {
		if calc := bitmapChecksum(b, fs.superblock.checksumSeed, fs.superblock.features.fs64Bit); gd.blockBitmapChecksum != calc {
			return nil, fmt.Errorf("checksum's do no match: expected %d, received %d", gd.blockBitmapChecksum, calc)
		}
	}

	return bs, nil
}

// writeBlockBitmap write the inode bitmap to the disk.
func (fs *FileSystem) writeBlockBitmap(bm *util.Bitmap, group int) error {
	if group >= len(fs.groupDescriptors.descriptors) {
		return fmt.Errorf("block group %d does not exist", group)
	}
	b := bm.ToBytes()
	gd := fs.groupDescriptors.descriptors[group]

	bitmapLocation := gd.blockBitmapLocation
	offset := int64(bitmapLocation*uint64(fs.superblock.blockSize) + uint64(fs.start))
	wrote, err := fs.file.WriteAt(b, offset)
	if err != nil {
		return fmt.Errorf("unable to write block bitmap for blockgroup %d: %w", gd.number, err)
	}
	if wrote != int(fs.superblock.blockSize) {
		return fmt.Errorf("wrote %d bytes instead of expected %d for block bitmap of block group %d", wrote, fs.superblock.blockSize, gd.number)
	}

	if fs.superblock.features.metadataChecksums {
		gd.blockBitmapChecksum = bitmapChecksum(b, fs.superblock.checksumSeed, fs.superblock.features.fs64Bit)
		if err = fs.writeGroupDescriptor(gd); err != nil {
			return err
		}
	}
	return nil
}

func (fs *FileSystem) writeSuperblock() error {
	superblockBytes, err := fs.superblock.toBytes()
	if err != nil {
		return fmt.Errorf("could not convert superblock to bytes: %v", err)
	}
	_, err = fs.file.WriteAt(superblockBytes, fs.start+int64(BootSectorSize))
	return err
}

func (fs *FileSystem) inodeBitmapByteLocation(gd *groupDescriptor) int64 {
	return int64(gd.inodeBitmapLocation*uint64(fs.superblock.blockSize) + uint64(fs.start))
}

func (fs *FileSystem) inodeByteLocation(gd *groupDescriptor, inodeNum uint32) int64 {
	// bytesStart is beginning byte for the inodeTableBlock
	byteStart := gd.inodeTableLocation*uint64(fs.superblock.blockSize) + uint64(fs.start)
	// offsetInode is how many inodes in our inode is
	offsetInode := inodeOffset(inodeNum, fs.superblock.inodesPerGroup)
	// offset is how many bytes in our inode is
	offset := offsetInode * uint32(fs.superblock.inodeSize)
	return int64(byteStart) + int64(offset)
}

func (fs *FileSystem) inodeNumFromBitmap(gd *groupDescriptor, bitmapLocation int) uint32 {
	return uint32(gd.number)*fs.superblock.inodesPerGroup + uint32(bitmapLocation) + 1
}

// where do we find the GDT?
//   - if blocksize is 1024, then 1024 padding for BootSector is block 0, 1024 for superblock is block 1
//     and then the GDT starts at block 2
//   - if blocksize is larger than 1024, then 1024 padding for BootSector followed by 1024 for superblock
//     is block 0, and then the GDT starts at block 1
func getGDTBlock(sb *superblock) int64 {
	if sb.blockSize == ext4MinBlockSize {
		return 2
	}
	return 1
}

func inodeOffset(inodeNumber, inodesPerGroup uint32) uint32 {
	return (inodeNumber - 1) % inodesPerGroup
}

func blockGroupForInode(inodeNumber, inodesPerGroup uint32) int {
	return int((inodeNumber - 1) / inodesPerGroup)
}
func blockGroupForBlock(blockNumber int, blocksPerGroup uint32) int {
	return (blockNumber - 1) / int(blocksPerGroup)
}

func blocksRequired(sizeInBytes int, bytesPerBlock uint32) uint32 {
	blocks := uint32(sizeInBytes) / bytesPerBlock
	if uint32(sizeInBytes)%bytesPerBlock > 0 {
		return blocks + 1
	}
	return blocks
}
