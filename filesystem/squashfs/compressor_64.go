//go:build !arm && !amd

package squashfs

import (
	"bytes"
	"fmt"
	"io"

	"github.com/ulikunitz/xz"
	"github.com/ulikunitz/xz/lzma"
)

func (c *CompressorLzma) compress(in []byte) ([]byte, error) {
	var b bytes.Buffer
	lz, err := lzma.NewWriter(&b)
	if err != nil {
		return nil, fmt.Errorf("error creating lzma compressor: %v", err)
	}
	if _, err := lz.Write(in); err != nil {
		return nil, err
	}
	if err := lz.Close(); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}
func (c *CompressorLzma) decompress(in []byte) ([]byte, error) {
	b := bytes.NewReader(in)
	lz, err := lzma.NewReader(b)
	if err != nil {
		return nil, fmt.Errorf("error creating lzma decompressor: %v", err)
	}
	p, err := io.ReadAll(lz)
	if err != nil {
		return nil, fmt.Errorf("error decompressing: %v", err)
	}
	return p, nil
}

func (c *CompressorXz) compress(in []byte) ([]byte, error) {
	var b bytes.Buffer
	xzWriter, err := xz.NewWriterConfig(&b, xz.WriterConfig{
		Workers: 2,
	})
	if err != nil {
		return nil, fmt.Errorf("error creating xz compressor: %v", err)
	}
	if _, err = xzWriter.Write(in); err != nil {
		return nil, err
	}
	if err = xzWriter.Close(); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func (c *CompressorXz) decompress(in []byte) ([]byte, error) {
	b := bytes.NewReader(in)
	xzReader, err := xz.NewReader(b)
	if err != nil {
		return nil, fmt.Errorf("error creating xz decompressor: %v", err)
	}
	p, err := io.ReadAll(xzReader)
	if err != nil {
		return nil, fmt.Errorf("error decompressing: %v", err)
	}
	return p, nil
}
