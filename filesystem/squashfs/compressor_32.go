//go:build arm || amd

// lzma and lz do not compile for 32bit systems
package squashfs

import (
	"errors"
)

func (c *CompressorLzma) compress(in []byte) ([]byte, error) {
	return nil, errors.New("not supported on 32 bit systems")
}
func (c *CompressorLzma) decompress(in []byte) ([]byte, error) {
	return nil, errors.New("not supported on 32 bit systems")
}

func (c *CompressorXz) compress(in []byte) ([]byte, error) {
	return nil, errors.New("not supported on 32 bit systems")
}
func (c *CompressorXz) decompress(in []byte) ([]byte, error) {
	return nil, errors.New("not supported on 32 bit systems")
}
