// coredns-s3dumper/uploader.go
package s3dumper

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"path"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

// S3Uploader handles uploading logs to S3.
type S3Uploader struct {
	Client *s3.Client
	Bucket string
	Prefix string
}

// Upload marshals, compresses, and uploads log entries to S3 in Parquet format.
func (u *S3Uploader) Upload(entries []*LogEntry) {
	if len(entries) == 0 {
		return
	}

	// 1. Create an in-memory buffer and a Parquet writer
	buf := new(bytes.Buffer)
	pw, err := writer.NewParquetWriter(buf, new(LogEntry), 4)
	if err != nil {
		log.Printf("[ERROR] s3dumper: failed to create in-memory parquet writer: %v", err)
		return
	}

	// 2. Configure Parquet writer properties
	pw.RowGroupSize = 128 * 1024 * 1024 // 128M
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	// 3. Write each entry
	for _, entry := range entries {
		if err = pw.Write(entry); err != nil {
			log.Printf("[ERROR] s3dumper: failed to write record to parquet buffer: %v", err)
		}
	}

	// 4. Close the writer to flush all data to the buffer
	if err = pw.Close(); err != nil {
		log.Printf("[ERROR] s3dumper: failed to close parquet writer: %v", err)
		return
	}

	// 5. Generate a unique key for the S3 object
	key := u.generateS3Key()

	// 6. Upload the buffer's content to S3
	_, err = u.Client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:      aws.String(u.Bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(buf.Bytes()),
		ContentType: aws.String("application/octet-stream"), // Use a generic content type for Parquet
	})

	if err != nil {
		log.Printf("[ERROR] s3dumper: failed to upload logs to S3 bucket %s with key %s: %v", u.Bucket, key, err)
	} else {
		log.Printf("[INFO] s3dumper: successfully uploaded %d log entries to s3://%s/%s", len(entries), u.Bucket, key)
	}
}

// generateS3Key creates a unique, time-partitioned key.
func (u *S3Uploader) generateS3Key() string {
	now := time.Now().UTC()
	uuid, _ := uuid.NewRandom()
	// Changed extension to .parquet
	filename := fmt.Sprintf("%d-%s.parquet", now.UnixNano(), uuid.String())
	return path.Join(
		u.Prefix,
		now.Format("2006"), // Year
		now.Format("01"),   // Month
		now.Format("02"),   // Day
		filename,
	)
}
