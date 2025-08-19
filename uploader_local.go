// coredns-s3dumper/uploader_local.go
package s3dumper

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
    "github.com/xitongsys/parquet-go/source"
)

// LocalUploader handles writing logs to the local filesystem.
type LocalUploader struct {
	Path string
}

// Upload implements the Uploader interface, writing data in Parquet format.
func (u *LocalUploader) Upload(entries []*LogEntry) {
    if len(entries) == 0 {
        return
    }

    // 1. Generate a unique filename with a .parquet extension
    filename := u.generateFilename()
    fullPath := filepath.Join(u.Path, filename)

    // 2. Ensure directory exists
    if err := os.MkdirAll(u.Path, 0755); err != nil {
        log.Printf("[ERROR] s3dumper: failed to create log directory %s: %v", u.Path, err)
        return
    }

    // 3. Create the file
    fw, err := os.Create(fullPath)
    if err != nil {
        log.Printf("[ERROR] s3dumper: failed to create local file %s: %v", fullPath, err)
        return
    }
    defer fw.Close()

    // 4. Create a file wrapper that implements source.ParquetFile interface
    pf := &ParquetFileWrapper{File: fw}

    // Use `new(LogEntry)` to create the schema from the struct tags
    // The final argument is the number of parallel writes
    pw, err := writer.NewParquetWriter(pf, new(LogEntry), 4)
    if err != nil {
        log.Printf("[ERROR] s3dumper: failed to create parquet writer for %s: %v", fullPath, err)
        return
    }

    // 5. Configure Parquet writer properties (e.g., compression)
    // Snappy is a great default for performance and good compression ratio.
    pw.RowGroupSize = 128 * 1024 * 1024 // 128M
    pw.CompressionType = parquet.CompressionCodec_SNAPPY

    // 6. Write each entry to the Parquet writer
    for _, entry := range entries {
        if err = pw.Write(entry); err != nil {
            log.Printf("[ERROR] s3dumper: failed to write record to parquet file %s: %v", fullPath, err)
            // Continue trying to write other records
        }
    }

    // 7. Close the writer to flush buffers and write the file footer
    if err = pw.WriteStop(); err != nil { // Use WriteStop instead of Close
        log.Printf("[ERROR] s3dumper: failed to close parquet writer for %s: %v", fullPath, err)
        return // Return early as the file is likely corrupt
    }

    log.Printf("[INFO] s3dumper: successfully wrote %d log entries to %s", len(entries), fullPath)
}

// ParquetFileWrapper wraps os.File to implement source.ParquetFile interface
type ParquetFileWrapper struct {
    *os.File
}

// Create method implementation for source.ParquetFile interface
func (p *ParquetFileWrapper) Create(name string) (source.ParquetFile, error) {
    file, err := os.Create(name)
    if err != nil {
        return nil, err
    }
    return &ParquetFileWrapper{File: file}, nil
}

// Open method implementation for source.ParquetFile interface
func (p *ParquetFileWrapper) Open(name string) (source.ParquetFile, error) {
    file, err := os.Open(name)
    if err != nil {
        return nil, err
    }
    return &ParquetFileWrapper{File: file}, nil
}

// generateFilename creates a unique, time-based filename.
// Example: 1698429600-uuid.json.gz
func (u *LocalUploader) generateFilename() string {
       now := time.Now().UTC()
       uuid, _ := uuid.NewRandom()
       return fmt.Sprintf("%d-%s.json.gz", now.UnixNano(), uuid.String())
}

