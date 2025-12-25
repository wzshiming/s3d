//go:build unix

package storage

import (
	"io"
	"os"
	"syscall"
)

const (
	// maxSendfileSize limits the amount of data sent in a single sendfile call.
	// Set to a value that safely fits in int on all platforms to avoid overflow.
	// On 32-bit systems, int max is ~2GB; on 64-bit it's much larger.
	// We use a conservative 1GB limit that works everywhere.
	maxSendfileSize = 1 << 30 // 1GB
)

// isSendfileUnsupported checks if the error indicates sendfile is not supported.
func isSendfileUnsupported(err error) bool {
	return err == syscall.EINVAL || err == syscall.ENOSYS ||
		err == syscall.ENOTSUP || err == syscall.EOPNOTSUPP
}

// copyFileWithSendfile copies data from src to dst using sendfile for better performance.
//
// This function uses the syscall.Sendfile system call, which performs zero-copy data transfer
// directly in kernel space between file descriptors, avoiding expensive user-space copies.
//
// Key behaviors:
//   - Transfers data in chunks (up to 1GB each) to avoid integer overflow on 32-bit systems
//   - Falls back to io.Copy if sendfile is not supported (EINVAL, ENOSYS, ENOTSUP, EOPNOTSUPP)
//   - Fallback only occurs if no data has been written yet to avoid partial data corruption
//   - Returns the number of bytes copied and any error encountered
//
// Parameters:
//   - dst: Destination file (must be opened for writing)
//   - src: Source file (must be opened for reading)
//
// Returns:
//   - Number of bytes successfully copied
//   - Error if the operation fails
func copyFileWithSendfile(dst *os.File, src *os.File) (int64, error) {
	// Get source file size
	srcInfo, err := src.Stat()
	if err != nil {
		return 0, err
	}
	
	srcSize := srcInfo.Size()
	if srcSize == 0 {
		return 0, nil
	}
	
	// Get file descriptors
	srcFd := int(src.Fd())
	dstFd := int(dst.Fd())
	
	var written int64
	remaining := srcSize
	
	// sendfile may not copy all data in one call, so we loop
	for remaining > 0 {
		// Limit the amount transferred per call to avoid integer overflow
		chunkSize := remaining
		if chunkSize > maxSendfileSize {
			chunkSize = maxSendfileSize
		}
		
		n, err := syscall.Sendfile(dstFd, srcFd, nil, int(chunkSize))
		if err != nil {
			// If sendfile is not supported, fall back to io.Copy
			// Only fall back if no data has been written yet
			if isSendfileUnsupported(err) && written == 0 {
				// Reset source file position before fallback
				// Note: dst position is already at the correct location since
				// written == 0 means sendfile hasn't modified it yet
				if _, seekErr := src.Seek(0, io.SeekStart); seekErr != nil {
					return 0, seekErr
				}
				// Sendfile not supported, use io.Copy instead
				return io.Copy(dst, src)
			}
			return written, err
		}
		
		// If sendfile returns 0, no more data can be transferred
		if n == 0 {
			break
		}
		
		written += int64(n)
		remaining -= int64(n)
	}
	
	return written, nil
}
