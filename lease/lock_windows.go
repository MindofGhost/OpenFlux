package lease

import (
	"os"

	"golang.org/x/sys/windows"
)

func lockFile(f *os.File) error {
	return windows.LockFileEx(windows.Handle(f.Fd()), windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, &windows.Overlapped{})
}

// Go's Windows rename replaces the state file after its contents were synced.
// Directory handles do not support the Unix directory-fsync operation.
func syncDirectory(string) error { return nil }
