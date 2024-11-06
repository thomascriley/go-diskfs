package ext4

import (
	"encoding/binary"
	"fmt"
	"slices"
)

// Directory represents a single directory in an ext4 filesystem
type directoryEntriesLinear struct {
	entries []*directoryEntry

	// super block flags
	bytesPerBlock uint32
	hasFileType   bool
	checkSum      checksummer

	// if true, the size of each entry needs to be recalculated
	dirty bool
}

func (d *directoryEntriesLinear) toBytes() []byte {
	bs := make([]byte, d.Size())
	_ = d.MarshalExt4(bs)
	return bs
}

func (d *directoryEntriesLinear) Entries() []*directoryEntry {
	return d.entries
}

// AddEntry adds the directory entry and returns the number of new blocks that are needed
func (d *directoryEntriesLinear) AddEntry(entry *directoryEntry) {
	d.entries = append(d.entries, entry)
	d.MarkDirty()
}

// RemoveEntry removes the directory entry and returns the number of used blocks that are no longer needed
func (d *directoryEntriesLinear) RemoveEntry(entry *directoryEntry) {
	d.entries = slices.DeleteFunc(d.entries, func(e *directoryEntry) bool { return e.inode == entry.inode })
	d.MarkDirty()
}

func (d *directoryEntriesLinear) MarkDirty() {
	d.dirty = true
}

func (d *directoryEntriesLinear) clean() {
	if !d.dirty {
		return
	}
	for _, entry := range d.entries {
		entry.length = uint16(entry.CalcSize())
	}
	d.dirty = false
}

func (d *directoryEntriesLinear) Size() int {
	var (
		size       uint32
		index      uint32
		needed     uint32
		available  uint32
		footerSize uint32
	)
	d.clean()
	if d.checkSum != nil {
		footerSize = uint32(minDirEntryLength)
	}
	for _, de := range d.entries {
		needed = uint32(de.Size())
		available = d.bytesPerBlock - index - footerSize

		// if adding this one will go past the end of the block, zero out the remaining bytes in the block and add the
		// fake checksum directory entry to the end of the leaf block
		if needed > available {
			size += d.bytesPerBlock
			index = 0
		}
		index += needed
	}
	if index > 0 {
		size += d.bytesPerBlock
	}
	return int(size)
}

func (d *directoryEntriesLinear) MarshalExt4(b []byte) (err error) {
	var (
		index         uint32
		needed        uint32
		blockOffset   uint32
		previousIndex uint32
	)
	d.clean()
	for i, de := range d.entries {
		needed = uint32(de.Size())

		// if adding this one will go past the end of the block, zero out the remaining bytes in the block and add the
		// fake checksum directory entry to the end of the leaf block
		if needed > d.availableBytes(index) {
			if i > 0 {
				d.extendDirectoryEntryLen(d.entries[i-1], b, blockOffset, previousIndex, index)
			}
			d.appendCheckSum(b[blockOffset : blockOffset+d.bytesPerBlock])
			index = 0
			blockOffset += d.bytesPerBlock
		}

		previousIndex = index
		index += needed
		if err = de.MarshalExt4(b[previousIndex+blockOffset : index+blockOffset]); err != nil {
			return err
		}
	}
	if index > 0 {
		if len(d.entries) > 0 {
			d.extendDirectoryEntryLen(d.entries[len(d.entries)-1], b, blockOffset, previousIndex, index)
		}
		d.appendCheckSum(b[blockOffset : blockOffset+d.bytesPerBlock])
	}
	return nil
}

func (d *directoryEntriesLinear) availableBytes(index uint32) uint32 {
	available := d.bytesPerBlock - index
	if d.checkSum != nil {
		available -= uint32(minDirEntryLength)
	}
	return available
}

func (d *directoryEntriesLinear) extendDirectoryEntryLen(entry *directoryEntry, bs []byte, blockOffset, prevIndex, index uint32) {
	available := uint16(d.availableBytes(index))
	if available == 0 {
		return
	}
	entry.length = available + uint16(entry.CalcSize())
	_ = entry.MarshalExt4(bs[blockOffset+prevIndex : blockOffset+index])
	for j := blockOffset + index; j < blockOffset+d.bytesPerBlock; j++ {
		bs[j] = 0
	}
}

func (d *directoryEntriesLinear) appendCheckSum(b []byte) {
	if d.checkSum == nil {
		return
	}
	offset := len(b) - minDirEntryLength
	binary.LittleEndian.PutUint32(b[offset:offset+4], 0)
	binary.LittleEndian.PutUint16(b[offset+4:offset+6], uint16(minDirEntryLength))
	b[offset+6] = 0
	b[offset+7] = 0xde
	binary.LittleEndian.PutUint32(b[offset+0x8:], d.checkSum(b[:offset]))
}

func (d *directoryEntriesLinear) UnmarshalExt4(b []byte) (err error) {
	d.entries = make([]*directoryEntry, 0, 4)

	// checksum if needed
	if d.checkSum != nil {
		var (
			newb                []byte
			checksumEntryOffset = int(d.bytesPerBlock) - minDirEntryLength
			checksumOffset      = int(d.bytesPerBlock) - 4
		)
		for i := 0; i < len(b); i += int(d.bytesPerBlock) {
			block := b[i : i+int(d.bytesPerBlock)]
			inBlockChecksum := block[checksumOffset:]
			block = block[:checksumEntryOffset]
			// save everything except the checksum
			newb = append(newb, block...)
			// checksum the entire block
			checksumValue := binary.LittleEndian.Uint32(inBlockChecksum)
			// checksum the block
			if actualChecksum := d.checkSum(block); actualChecksum != checksumValue {
				return fmt.Errorf("directory block checksum mismatch: expected %x, got %x", checksumValue, actualChecksum)
			}
		}
		b = newb
	}

	// convert into directory entries
	count := 0
	var length uint16
	for i := 0; i < len(b); count++ {
		// read the length of the entry
		if length = binary.LittleEndian.Uint16(b[i+0x4 : i+0x6]); length == 0 {
			// the rest is padding to fill the block
			break
		}
		entry := &directoryEntry{
			hasFileType: d.hasFileType,
		}
		if err = entry.UnmarshalExt4(b[i : i+int(length)]); err != nil {
			return fmt.Errorf("failed to parse directory entry %d: %v", count, err)
		}

		d.entries = append(d.entries, entry)
		i += int(length)
	}

	return nil
}
