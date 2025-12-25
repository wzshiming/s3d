//go:build !unix

package storage

import (
	"io"
	"os"
)

// copyFileWithSendfile is a fallback implementation for non-Unix systems.
// It uses standard io.Copy since sendfile is not available.
func copyFileWithSendfile(dst *os.File, src *os.File) (int64, error) {
	return io.Copy(dst, src)
}
