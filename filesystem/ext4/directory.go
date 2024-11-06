package ext4

/*
const (
	directoryHashTreeRootMinSize = 0x28
	directoryHashTreeNodeMinSize = 0x12
)
*/

// Directory represents a single directory in an ext4 filesystem
type Directory struct {
	directoryEntry
	root    bool
	entries directoryEntries
}

/*
type directoryBase struct {
	directoryEntry
	root bool
}

// toBytes convert our entries to raw bytes. Provides checksum as well. Final returned byte slice will be a multiple of bytesPerBlock.
// TODO: check is the parent directory is hashed and serialize as such if necessary
// https://ext4.wiki.kernel.org/index.php/Ext4_Disk_Layout#Hash_Tree_Directories
func (d *Directory) toBytes(bytesPerBlock uint32, checksumEnabled, isHashTree bool, numExtents int, checksumFunc checksumAppender) []byte {
	if isHashTree {
		return d.toHashTreeBytes(bytesPerBlock, checksumEnabled, checksumFunc)
	} else {
		return d.toLinearBytes(bytesPerBlock, checksumEnabled, checksumFunc)
	}
}

func (d *Directory) toLinearBytes(bytesPerBlock uint32, checksumEnabled bool, checksumFunc checksumAppender) []byte {
	b := make([]byte, 0)
	var (
		index      uint32
		needed     uint32
		available  uint32
		footerSize = uint32(minDirEntryLength)
		block      = make([]byte, bytesPerBlock)
		zeros      = make([]byte, bytesPerBlock)
	)
	for _, de := range d.entries {
		needed = uint32(de.Size())
		available = bytesPerBlock - index - footerSize

		// if adding this one will go past the end of the block, zero out the remaining bytes in the block and add the
		// fake checksum directory entry to the end of the leaf block
		if needed > available {
			copy(block[index:bytesPerBlock-footerSize], zeros[:available])
			if checksumEnabled {
				checksumFunc(block)
			}
			b = slices.Grow(b, 4096)
			b = append(b, block...)
			index = 0
		}

		_ = de.MarshalExt4(block[index : index+needed])
		index += needed
	}
	if index > 0 {
		b = slices.Grow(b, 4096)
		b = append(b, block[:index]...)
		b = append(b, zeros[:bytesPerBlock-index]...)
		if checksumEnabled {
			checksumFunc(b[len(b)-int(bytesPerBlock):])
		}
	}
	return b
}

// toHashTreeBytes
// as defined in https://ext4.wiki.kernel.org/index.php/Ext4_Disk_Layout#Hash_Tree_Directories
func (d *Directory) toHashTreeBytes(bytesPerBlock uint32, checksumEnabled bool, checksumFunc checksumAppender) []byte {
	b := make([]byte, 4096)
	binary.LittleEndian.PutUint32(b[:0x4], d.inode)
	binary.LittleEndian.PutUint16(b[0x4:0x6], 12)
	b[0x6] = 1
	b[0x7] = uint8(dirFileTypeDirectory)
	b[0x8] = '.'

	for _, e := range d.entries {
		if e.filename == ".." {
			binary.LittleEndian.PutUint32(b[0xc:0x10], e.inode)
			binary.LittleEndian.PutUint16(b[0x10:0x12], 12)
			b[0x12] = 2
			b[0x13] = uint8(dirFileTypeDirectory)
			b[0x14] = '.'
			b[0x15] = '.'
			break
		}
	}

	// matches create super block hash version. is the the supported default? most recent?
	b[0x1c] = byte(hashHalfMD4)
	b[0x1d] = 0x8                                                                 // length of the tree
	b[0x1e] = 0x00                                                                // tree depth TODO: determine the tree depth from the content (number of leaf nodes?), for now just set 0x00 and cross fingers
	binary.LittleEndian.PutUint16(b[0x20:0x22], uint16((bytesPerBlock-0x28)/8)+1) // Maximum number of dx_entries that can follow this header, plus 1 for the header itself.
	binary.LittleEndian.PutUint16(b[0x22:0x24], uint16(len(d.entries)))           // Actual number of dx_entries that follow this header, plus 1 for the header itself.
}

type directoryHashRoot struct {
	inodeDir      uint32
	inodeParent   uint32
	hashVersion   hashVersion
	limit         uint16
	depth         uint8
	blockNumber   uint32
	hashAlgorithm hashAlgorithm
	childEntries  []directoryHashEntry
	dotEntry      *directoryEntry
	dotDotEntry   *directoryEntry
}

func (d *directoryHashRoot) entries() []directoryHashEntry {
	return d.childEntries
}

// parseDirectoryTreeRoot parses the directory hash tree root from the given byte slice. Reads only the root node.
func parseDirectoryTreeRoot(b []byte, largeDir bool) (node *directoryHashRoot, err error) {
	// min size
	if len(b) < directoryHashTreeRootMinSize {
		return nil, fmt.Errorf("directory hash tree root is too small")
	}

	// dot parameters
	dotInode := binary.LittleEndian.Uint32(b[0x0:0x4])
	dotSize := binary.LittleEndian.Uint16(b[0x4:0x6])
	if dotSize != 12 {
		return nil, fmt.Errorf("directory hash tree root dot size is %d and not 12", dotSize)
	}
	dotNameSize := b[0x6]
	if dotNameSize != 1 {
		return nil, fmt.Errorf("directory hash tree root dot name length is %d and not 1", dotNameSize)
	}
	dotFileType := directoryFileType(b[0x7])
	if dotFileType != dirFileTypeDirectory {
		return nil, fmt.Errorf("directory hash tree root dot file type is %d and not %v", dotFileType, dirFileTypeDirectory)
	}
	dotName := b[0x8:0xc]
	if !bytes.Equal(dotName, []byte{'.', 0, 0, 0}) {
		return nil, fmt.Errorf("directory hash tree root dot name is %s and not '.'", dotName)
	}

	// dotdot parameters
	dotdotInode := binary.LittleEndian.Uint32(b[0xc:0x10])
	dotdotNameSize := b[0x12]
	if dotdotNameSize != 2 {
		return nil, fmt.Errorf("directory hash tree root dotdot name length is %d and not 2", dotdotNameSize)
	}
	dotdotFileType := directoryFileType(b[0x13])
	if dotdotFileType != dirFileTypeDirectory {
		return nil, fmt.Errorf("directory hash tree root dotdot file type is %d and not %v", dotdotFileType, dirFileTypeDirectory)
	}
	dotdotName := b[0x14:0x18]
	if !bytes.Equal(dotdotName, []byte{'.', '.', 0, 0}) {
		return nil, fmt.Errorf("directory hash tree root dotdot name is %s and not '..'", dotdotName)
	}

	treeInformation := b[0x1d]
	if treeInformation != 8 {
		return nil, fmt.Errorf("directory hash tree root tree information is %d and not 8", treeInformation)
	}
	treeDepth := b[0x1e]
	// there are maximums for this
	maxTreeDepth := uint8(2)
	if largeDir {
		maxTreeDepth = 3
	}
	if treeDepth > maxTreeDepth {
		return nil, fmt.Errorf("directory hash tree root tree depth is %d and not between 0 and %d", treeDepth, maxTreeDepth)
	}

	dxEntriesLimit := binary.LittleEndian.Uint16(b[0x20:0x22])
	dxEntriesCount := binary.LittleEndian.Uint16(b[0x22:0x24])
	blockNumber := binary.LittleEndian.Uint32(b[0x24:0x28])

	node = &directoryHashRoot{
		inodeDir:      binary.LittleEndian.Uint32(b[0x0:0x4]),
		inodeParent:   binary.LittleEndian.Uint32(b[0xC:0x10]),
		hashAlgorithm: hashAlgorithm(b[0x1c]), // what hashing algorithm is used?
		depth:         treeDepth,
		blockNumber:   blockNumber,
		limit:         dxEntriesLimit,
		childEntries:  make([]directoryHashEntry, 0, int(dxEntriesCount)),
		dotEntry: &directoryEntry{
			inode:    dotInode,
			fileType: dotFileType,
			filename: ".",
		},
		dotDotEntry: &directoryEntry{
			inode:    dotdotInode,
			fileType: dotdotFileType,
			filename: "..",
		},
	}

	// remove 1, because the count includes the one in the dx_root itself
	node.childEntries = append(node.childEntries, directoryHashEntry{hash: 0, block: binary.LittleEndian.Uint32(b[0x24:0x28])})
	for i := 0; i < int(dxEntriesCount)-1; i++ {
		entryOffset := 0x28 + (i * 8)
		hash := binary.LittleEndian.Uint32(b[entryOffset : entryOffset+4])
		block := binary.LittleEndian.Uint32(b[entryOffset+4 : entryOffset+8])
		node.childEntries = append(node.childEntries, directoryHashEntry{hash: hash, block: block})
	}

	return node, nil
}

// parseDirectoryTreeNode parses an internal directory hash tree node from the given byte slice. Reads only the node.
func parseDirectoryTreeNode(b []byte) (node *directoryHashNode, err error) {
	// min size
	if len(b) < directoryHashTreeNodeMinSize {
		return nil, fmt.Errorf("directory hash tree root is too small")
	}

	dxEntriesCount := binary.LittleEndian.Uint16(b[0xa:0xc])

	node = &directoryHashNode{
		childEntries: make([]directoryHashEntry, 0, int(dxEntriesCount)),
	}
	node.childEntries = append(node.childEntries, directoryHashEntry{hash: 0, block: binary.LittleEndian.Uint32(b[0xc:0x10])})
	for i := 0; i < int(dxEntriesCount)-1; i++ {
		entryOffset := 0x10 + (i * 8)
		hash := binary.LittleEndian.Uint32(b[entryOffset : entryOffset+4])
		block := binary.LittleEndian.Uint32(b[entryOffset+4 : entryOffset+8])
		node.childEntries = append(node.childEntries, directoryHashEntry{hash: hash, block: block})
	}

	return node, nil
}
*/
