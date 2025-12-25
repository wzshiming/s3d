//go:build unix

package storage

import (
	"io"
	"os"
	"syscall"
)

const (
	// maxSendfileSize limits the amount of data sent in a single sendfile call
	// to avoid integer overflow on 32-bit systems and to work around kernel limitations
	maxSendfileSize = 1 << 30 // 1GB
)

// copyFileWithSendfile copies data from src to dst using sendfile for better performance.
// It returns the number of bytes copied and any error encountered.
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
			if (err == syscall.EINVAL || err == syscall.ENOSYS) && written == 0 {
				// Reset source file position before fallback
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
