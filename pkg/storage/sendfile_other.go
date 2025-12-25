//go:build !unix

package storage

import (
	"io"
	"os"
)

// copyFileWithSendfile is a fallback implementation for non-Unix systems.
//
// On systems that don't support Unix syscalls (e.g., Windows), this function
// provides a compatible interface using standard io.Copy instead of sendfile.
// While it doesn't provide zero-copy optimization, it ensures the code works
// correctly on all platforms.
//
// Parameters:
//   - dst: Destination file (must be opened for writing)
//   - src: Source file (must be opened for reading)
//
// Returns:
//   - Number of bytes successfully copied
//   - Error if the operation fails
func copyFileWithSendfile(dst *os.File, src *os.File) (int64, error) {
	return io.Copy(dst, src)
}
