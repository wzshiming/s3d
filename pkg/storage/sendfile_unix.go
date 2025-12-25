//go:build unix

package storage

import (
	"io"
	"os"
	"syscall"
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
		n, err := syscall.Sendfile(dstFd, srcFd, nil, int(remaining))
		if err != nil {
			// If sendfile is not supported or fails, fall back to io.Copy
			if err == syscall.EINVAL || err == syscall.ENOSYS {
				// Seek back to beginning
				if _, seekErr := src.Seek(0, io.SeekStart); seekErr != nil {
					return written, seekErr
				}
				// Use io.Copy for the remaining data
				copied, copyErr := io.Copy(dst, src)
				return written + copied, copyErr
			}
			return written, err
		}
		
		written += int64(n)
		remaining -= int64(n)
		
		if n == 0 {
			break
		}
	}
	
	return written, nil
}
