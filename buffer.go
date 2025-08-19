// coredns-s3dumper/buffer.go
package s3dumper

import "sync"

// LogBuffer holds log entries before they are flushed.
type LogBuffer struct {
	mu      sync.Mutex
	entries []*LogEntry
	maxSize int
}

// NewLogBuffer creates a new buffer.
func NewLogBuffer(maxSize int) *LogBuffer {
	return &LogBuffer{
		entries: make([]*LogEntry, 0, maxSize),
		maxSize: maxSize,
	}
}

// Add appends a log entry to the buffer. If the buffer is full,
// it returns the current batch of entries for flushing.
func (b *LogBuffer) Add(entry *LogEntry) []*LogEntry {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.entries = append(b.entries, entry)
	if len(b.entries) >= b.maxSize {
		return b.resetAndReturn()
	}
	return nil
}

// Flush returns all entries currently in the buffer and clears it.
func (b *LogBuffer) Flush() []*LogEntry {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.resetAndReturn()
}

// resetAndReturn returns the current entries and creates a new slice.
// This must be called within a lock.
func (b *LogBuffer) resetAndReturn() []*LogEntry {
	if len(b.entries) == 0 {
		return nil
	}
	// Copy to a new slice to release the lock quickly
	toFlush := b.entries
	b.entries = make([]*LogEntry, 0, b.maxSize)
	return toFlush
}

