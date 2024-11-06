package ext4

import (
	"encoding/binary"
	"fmt"
	"slices"
)

const (
	directoryHashEntrySize          = 0x8
	directoryHashTreeRootHeaderSize = 0x20
)

type directoryHashEntry struct {
	hash  uint32
	block uint32
}

func (d *directoryHashEntry) Size() int { return directoryHashEntrySize }

func (d *directoryHashEntry) UnmarshalExt4(b []byte) error {
	d.hash = binary.LittleEndian.Uint32(b[:0x4])
	d.block = binary.LittleEndian.Uint32(b[0x4:0x8])
	return nil
}

func (d *directoryHashEntry) MarshalExt4(b []byte) error {
	binary.LittleEndian.PutUint32(b[:0x4], d.hash)
	binary.LittleEndian.PutUint32(b[0x4:0x8], d.block)
	return nil
}

type directoryHashTail struct {
	reserved uint32
	sum      uint32
}

func (d *directoryHashTail) Size() int { return directoryHashEntrySize }

func (d *directoryHashTail) UnmarshalExt4(b []byte) error {
	d.reserved = binary.LittleEndian.Uint32(b[:0x4])
	d.sum = binary.LittleEndian.Uint32(b[0x4:0x8])
	return nil
}

func (d *directoryHashTail) MarshalExt4(b []byte) error {
	binary.LittleEndian.PutUint32(b[:0x4], d.reserved)
	binary.LittleEndian.PutUint32(b[0x4:0x8], d.sum)
	return nil
}

type directoryHashNode struct {
	limit       uint16
	depth       uint8
	hashEntries []*directoryHashEntry
	entries     []*directoryEntry
	nodes       []directoryEntries
	tail        *directoryHashTail

	// root flags
	hashAlgorithm hashVersion
	major         bool

	// superblock flags
	linearCheckSum   checksummer
	hashTreeCheckSum checksummerHash
	hasFileType      bool
	bytesPerBlock    uint32
	hashTreeSeed     []uint32
}

func (d *directoryHashNode) Entries() []*directoryEntry { return d.entries }

func (d *directoryHashNode) entryHash(entry *directoryEntry) uint32 {
	majorHash, minorHash := ext4fsDirhash(entry.filename, d.hashAlgorithm, d.hashTreeSeed)
	if d.major {
		return majorHash
	}
	return minorHash
}

func (d *directoryHashNode) AddEntry(entry *directoryEntry) {
	// blocks are calculated during serialization for new entries

	hash := d.entryHash(entry)
	added := false
	for i := range d.hashEntries {
		fmt.Printf("Hash: %d -> %d : %d\n", d.hashEntries[i].hash, d.hashEntries[i+1].hash, hash)
		if !(hash >= d.hashEntries[i].hash && (i == len(d.hashEntries)-1 || hash < d.hashEntries[i+1].hash)) {
			continue
		}
		fmt.Println("adding entry")
		blocks := blocksRequired(d.nodes[i].Size(), d.bytesPerBlock)
		d.nodes[i].AddEntry(entry)
		d.entries = append(d.entries, entry)
		added = true
		if blocks != blocksRequired(d.nodes[i].Size(), d.bytesPerBlock) {
			if linear, ok := d.nodes[i].(*directoryEntriesLinear); ok {
				half := len(linear.entries) / 2
				additional := &directoryEntriesLinear{
					entries:       linear.entries[half:],
					bytesPerBlock: linear.bytesPerBlock,
					hasFileType:   linear.hasFileType,
					checkSum:      linear.checkSum,
				}
				linear.entries = linear.entries[:half]
				d.nodes = append(d.nodes, additional)
				d.hashEntries = append(d.hashEntries, &directoryHashEntry{
					hash:  d.entryHash(additional.entries[0]),
					block: uint32(len(d.hashEntries)) + 1,
				})
			}
		}
		break
	}
	if !added {
		fmt.Println("did not add entry!")
	}
}

func (d *directoryHashNode) RemoveEntry(entry *directoryEntry) {
	hash := d.entryHash(entry)
	for i := range d.hashEntries {
		if hash >= d.hashEntries[i].hash && (i == len(d.hashEntries)-1 || d.hashEntries[i+1].hash > hash) {
			blocks := blocksRequired(d.nodes[i].Size(), d.bytesPerBlock)
			d.nodes[i].RemoveEntry(entry)
			if len(d.nodes) > 1 && blocks != blocksRequired(d.nodes[i].Size(), d.bytesPerBlock) {
				if i == 0 {
					d.hashEntries[i+1].hash = d.hashEntries[i].hash
				}
				d.nodes = slices.Delete(d.nodes, i, i+1)
				d.hashEntries = slices.Delete(d.hashEntries, i, i+1)
			}
			break
		}
	}

	for i := range d.entries {
		if d.entries[i].inode == entry.inode {
			d.entries = slices.Delete(d.entries, i, i+1)
			return
		}
	}
}

func (d *directoryHashNode) HashEntries() []*directoryHashEntry { return d.hashEntries }

func (d *directoryHashNode) NodeSize() (size int) {
	for _, node := range d.nodes {
		size += node.Size()
	}
	return size
}

func (d *directoryHashNode) Size() int {
	// header plus content of nested nodes (both linear and hashed)
	return 0x8 + len(d.hashEntries)*directoryHashEntrySize
}

func (d *directoryHashNode) MarshalExt4(b []byte) (err error) {
	if size := d.Size(); len(b) < size {
		return fmt.Errorf("expected at least %d bytes, got %d", size, len(b))
	}

	// marshal the metadata
	binary.LittleEndian.PutUint16(b[0x0:0x2], d.limit)
	binary.LittleEndian.PutUint16(b[0x2:0x4], uint16(len(d.hashEntries)))
	binary.LittleEndian.PutUint32(b[0x4:0x8], d.hashEntries[0].block)

	// marshal the dir hash entries
	for i := 1; i < len(d.hashEntries); i++ {
		start := 0x8 + (i-1)*directoryHashEntrySize
		stop := start + directoryHashEntrySize
		if err = d.hashEntries[i].MarshalExt4(b[start:stop]); err != nil {
			return err
		}
	}
	return nil
}

func (d *directoryHashNode) MarshalExt4Nodes(b []byte) (err error) {
	// marshal the following blocks
	for i := 0; i < len(d.nodes); i++ {
		entry := d.hashEntries[i]
		start := d.bytesPerBlock * entry.block
		stop := start + d.bytesPerBlock
		if err = d.nodes[i].MarshalExt4(b[start:stop]); err != nil {
			return err
		}
	}
	return nil
}

// UnmarshalExt4 parses an internal directory hash tree node from the given byte slice. Reads only the node.
func (d *directoryHashNode) UnmarshalExt4(b []byte) (err error) {
	// min size
	if len(b) < directoryHashEntrySize {
		return fmt.Errorf("expected at least %d bytes, received: %d", directoryHashEntrySize, len(b))
	}

	d.limit = binary.LittleEndian.Uint16(b[0x0:0x2])
	d.hashEntries = make([]*directoryHashEntry, int(binary.LittleEndian.Uint16(b[0x2:0x4])))

	d.hashEntries[0] = &directoryHashEntry{
		hash:  0,
		block: binary.LittleEndian.Uint32(b[0x4:0x8]),
	}
	for i := 1; i < len(d.hashEntries); i++ {
		d.hashEntries[i] = &directoryHashEntry{}
		start := 0x8 + (i-1)*directoryHashEntrySize
		stop := start + directoryHashEntrySize
		if err = d.hashEntries[i].UnmarshalExt4(b[start:stop]); err != nil {
			return err
		}
	}
	return nil
}

func (d *directoryHashNode) UnmarshalExt4Nodes(b []byte) (err error) {
	d.nodes = make([]directoryEntries, len(d.hashEntries))

	var start uint32
	for i, entry := range d.hashEntries {
		if d.depth == 0 {
			d.nodes[i] = &directoryEntriesLinear{
				bytesPerBlock: d.bytesPerBlock,
				hasFileType:   d.hasFileType,
				checkSum:      d.linearCheckSum,
			}
		} else {
			// recursively parse the next level of the tree
			// read the next level down
			// skip the first 0x8 bytes as they are fake / empty to look like a linear directory entry for backward compatibility
			d.nodes[i] = &directoryInteriorHashNode{
				directoryHashNode: directoryHashNode{
					depth:            d.depth - 1,
					major:            !d.major,
					hashAlgorithm:    d.hashAlgorithm,
					hashTreeSeed:     d.hashTreeSeed,
					hashTreeCheckSum: d.hashTreeCheckSum,
					linearCheckSum:   d.linearCheckSum,
				},
			}
		}

		start = entry.block * d.bytesPerBlock
		if err = d.nodes[i].UnmarshalExt4(b[start : start+d.bytesPerBlock]); err != nil {
			return fmt.Errorf("error parsing linear directory entries: %w", err)
		}
	}

	d.entries = make([]*directoryEntry, 0)
	for _, node := range d.nodes {
		d.entries = append(d.entries, node.Entries()...)
	}
	return nil
}

func (d *directoryHashNode) UnmarshalExt4Tail(b []byte, offset int) error {
	if d.hashTreeCheckSum == nil {
		return nil
	}
	d.tail = &directoryHashTail{}
	tail := offset + int(d.limit)*directoryHashEntrySize
	return d.tail.UnmarshalExt4(b[tail : tail+d.tail.Size()])
}

func (d *directoryHashNode) MarshalExt4Tail(b []byte, offset int) error {
	if d.hashTreeCheckSum == nil {
		return nil
	}
	stop := offset + len(d.hashEntries)*directoryHashEntrySize
	tail := offset + int(d.limit)*directoryHashEntrySize
	// the sum must include the tail
	if err := d.tail.MarshalExt4(b[tail : tail+d.tail.Size()]); err != nil {
		return err
	}
	d.tail.sum = d.hashTreeCheckSum(b[0x0:stop], b[tail:tail+d.tail.Size()])
	err := d.tail.MarshalExt4(b[tail : tail+d.tail.Size()])
	return err
}

func (d *directoryHashNode) verifyCheckSum(b []byte, offset int) error {
	if d.hashTreeCheckSum == nil {
		return nil
	}
	stop := offset + len(d.hashEntries)*directoryHashEntrySize
	tail := offset + int(d.limit)*directoryHashEntrySize
	if expectedCheckSum := d.hashTreeCheckSum(b[0x0:stop], b[tail:tail+d.tail.Size()]); d.tail.sum != expectedCheckSum {
		return fmt.Errorf("checksums do not match, actual: %d, expected: %d", d.tail.sum, expectedCheckSum)
	}
	return nil
}

type directoryEntriesHashTreeRoot struct {
	directoryHashNode
	inodeDir    uint32
	inodeParent uint32
	treeInfoLen uint8
	depth       uint8
	blockNumber uint32
	dotEntry    *directoryEntry
	dotDotEntry *directoryEntry
	reserved    uint32
	tail        *directoryHashTail

	entries []*directoryEntry

	// feature flags from superblock
	large bool

	// if true then the entries list has been changed and we need to recalculate structure size and format
	dirty bool
}

func (d *directoryEntriesHashTreeRoot) Root() *directoryEntriesHashTreeRoot { return d }

func (d *directoryEntriesHashTreeRoot) Entries() []*directoryEntry {
	return d.entries
}

func (d *directoryEntriesHashTreeRoot) Size() int {
	return int(d.bytesPerBlock) + d.directoryHashNode.NodeSize()
}

func (d *directoryEntriesHashTreeRoot) MarkDirty() {
	d.dirty = true
}

func (d *directoryEntriesHashTreeRoot) AddEntry(de *directoryEntry) {
	if d.entries == nil {
		d.entries = make([]*directoryEntry, 0)
	}
	d.entries = append(d.entries, de)
	d.directoryHashNode.AddEntry(de)
}

func (d *directoryEntriesHashTreeRoot) RemoveEntry(de *directoryEntry) {
	for i := range d.entries {
		if d.entries[i].inode == de.inode {
			d.entries = slices.Delete(d.entries, i, i+1)
			return
		}
	}
}

// MarshalExt4 as defined in https://ext4.wiki.kernel.org/index.php/Ext4_Disk_Layout#Hash_Tree_Directories
func (d *directoryEntriesHashTreeRoot) MarshalExt4(b []byte) (err error) {
	if size := d.Size(); len(b) < size {
		return fmt.Errorf("expecting byte slice of size %d, received: %d", size, len(b))
	}

	if err = d.dotEntry.MarshalExt4(b); err != nil {
		return fmt.Errorf("failed to marshal dot entry: %w", err)
	}
	if err = d.dotDotEntry.MarshalExt4(b[0xc:]); err != nil {
		return fmt.Errorf("failed to marshal dot dot entry: %w", err)
	}

	// reserved
	binary.LittleEndian.PutUint32(b[0x18:], 0)

	// matches create super block hash version.
	b[0x1c] = byte(d.hashAlgorithm)

	// length of the tree (should be 0x8)
	b[0x1d] = 0x8

	// tree depth
	b[0x1e] = d.depth

	// 0xf -> unused flags

	stop := d.bytesPerBlock
	if d.hashTreeCheckSum != nil {
		stop = d.bytesPerBlock - uint32(directoryHashEntrySize)
	}
	if err = d.directoryHashNode.MarshalExt4(b[0x20:stop]); err != nil {
		return err
	}
	if err = d.MarshalExt4Tail(b, 0x20); err != nil {
		return err
	}
	return d.MarshalExt4Nodes(b)
}

// UnmarshalExt4 parses the directory hash tree root from the given byte slice. Reads only the root node.
func (d *directoryEntriesHashTreeRoot) UnmarshalExt4(b []byte) (err error) {
	// min size
	if size := d.directoryHashNode.Size(); len(b) < size {
		return fmt.Errorf("expecting byte slice of size %d, received: %d", size, len(b))
	}

	// current directory entry
	d.dotEntry = &directoryEntry{hasFileType: d.hasFileType}
	if err = d.dotEntry.UnmarshalExt4(b[:0xc]); err != nil {
		return err
	}

	// parent directory entry
	d.dotDotEntry = &directoryEntry{hasFileType: d.hasFileType}
	if err = d.dotDotEntry.UnmarshalExt4(b[0xc:0x18]); err != nil {
		return err
	}

	// reserved zero
	if d.reserved = binary.LittleEndian.Uint32(b[0x18:0x1c]); d.reserved != 0 {
		return fmt.Errorf("exepecting reserved bit to be zero, recevied: %d", d.reserved)
	}

	// hash version
	d.hashAlgorithm = hashVersion(b[0x1c]) // what hashing algorithm is used?

	// length of the tree information
	if d.treeInfoLen = b[0x1d]; d.treeInfoLen != 0x8 {
		return fmt.Errorf("directory hash tree root tree information is %d and not 8", d.treeInfoLen)
	}

	// indirect levels == depth of the htree. Cannot be larger than 3 if the INCOMPAT_LARGEDIR feature is set; cannot be larger than 2 otherwise.
	maxTreeDepth := uint8(2)
	if d.large {
		maxTreeDepth = 3
	}
	if d.depth = b[0x1e]; d.depth > maxTreeDepth {
		return fmt.Errorf("directory hash tree root tree depth is %d and not between 0 and %d", d.depth, maxTreeDepth)
	}

	// 0xf -> unused flags

	if err = d.directoryHashNode.UnmarshalExt4(b[0x20:d.bytesPerBlock]); err != nil {
		return fmt.Errorf("failed to decode interior hash tree node: %w", err)
	}

	if err = d.UnmarshalExt4Tail(b, 0x20); err != nil {
		return err
	}
	if err = d.verifyCheckSum(b, 0x20); err != nil {
		return err
	}
	if err = d.UnmarshalExt4Nodes(b); err != nil {
		return err
	}

	nested := d.directoryHashNode.Entries()
	entries := make([]*directoryEntry, 0, len(nested)+2)
	entries = append(entries, d.dotEntry, d.dotDotEntry)
	d.entries = append(entries, nested...)

	return nil
}

type directoryInteriorHashNode struct {
	directoryHashNode
	tail *directoryHashTail
}

func (d *directoryInteriorHashNode) Size() int {
	return int(d.bytesPerBlock) + d.directoryHashNode.NodeSize()
}

func (d *directoryInteriorHashNode) MarshalExt4(b []byte) (err error) {
	if size := d.Size(); len(b) < size {
		return fmt.Errorf("expecting byte slice of size %d, received: %d", size, len(b))
	}
	binary.LittleEndian.PutUint32(b[0x0:0x4], 0)
	binary.LittleEndian.PutUint32(b[0x4:0x8], 0)
	if err = d.directoryHashNode.MarshalExt4(b[0x8:d.bytesPerBlock]); err != nil {
		return err
	}
	if err = d.MarshalExt4Tail(b, 0x8); err != nil {
		return err
	}
	return d.MarshalExt4Nodes(b)
}

// UnmarshalExt4 parses an internal directory hash tree node from the given byte slice. Reads only the node.
func (d *directoryInteriorHashNode) UnmarshalExt4(b []byte) (err error) {
	if size := d.directoryHashNode.Size(); len(b) < size {
		return fmt.Errorf("expecting byte slice of size %d, received: %d", size, len(b))
	}
	if err = d.directoryHashNode.UnmarshalExt4(b[0x8:d.bytesPerBlock]); err != nil {
		return err
	}
	if err = d.UnmarshalExt4Tail(b, 0x8); err != nil {
		return err
	}
	if err = d.verifyCheckSum(b, 0x8); err != nil {
		return err
	}
	return d.UnmarshalExt4Nodes(b)
}
