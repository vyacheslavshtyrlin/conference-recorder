//go:build windows

package recording

// Windows rename durability is provided by the file flush followed by the
// atomic rename. Opening a directory for FlushFileBuffers is not portable.
func syncDirectory(string) error { return nil }
