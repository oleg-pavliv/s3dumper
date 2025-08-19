// coredns-s3dumper/setup.go
package s3dumper

import (
	_"context"
	"log"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/coredns/caddy"
	"github.com/coredns/coredns/core/dnsserver"
	"github.com/coredns/coredns/plugin"
)

const (
	defaultBatchSize     = 1000
	defaultFlushInterval = 30 * time.Second
)

// init registers this plugin.
func init() { plugin.Register("s3dumper", setup) }

// setup configures the plugin from the Corefile.
func setup(c *caddy.Controller) error {
	dumper, err := parseConfig(c)
	if err != nil {
		return plugin.Error("s3dumper", err)
	}

	dumper.Start()
	c.OnShutdown(func() error {
		return dumper.Shutdown()
	})

	dnsserver.GetConfig(c).AddPlugin(func(next plugin.Handler) plugin.Handler {
		dumper.Next = next
		return dumper
	})

	return nil
}

func parseConfig(c *caddy.Controller) (*S3Dumper, error) {
	// Default settings
	batchSize := defaultBatchSize
	flushInterval := defaultFlushInterval

	// Storage-specific settings
	storageType := "s3" // Default to s3
	bucket := ""
	prefix := "coredns-logs"
	region := ""
	localPath := ""

	for c.Next() { // Skip the plugin name
		for c.NextBlock() {
			switch c.Val() {
			// Common settings
			case "batch_size":
				if !c.NextArg() {
					return nil, c.ArgErr()
				}
				size, err := strconv.Atoi(c.Val())
				if err != nil {
					return nil, c.Errf("invalid batch_size: %v", err)
				}
				batchSize = size
			case "flush_interval":
				if !c.NextArg() {
					return nil, c.ArgErr()
				}
				interval, err := time.ParseDuration(c.Val())
				if err != nil {
					return nil, c.Errf("invalid flush_interval: %v", err)
				}
				flushInterval = interval

			// Storage type selector
			case "storage_type":
				if !c.NextArg() {
					return nil, c.ArgErr()
				}
				storageType = c.Val() // "s3" or "local"

			// S3 specific settings
			case "bucket":
				if !c.NextArg() {
					return nil, c.ArgErr()
				}
				bucket = c.Val()
			case "prefix":
				if !c.NextArg() {
					return nil, c.ArgErr()
				}
				prefix = c.Val()
			case "region":
				if !c.NextArg() {
					return nil, c.ArgErr()
				}
				region = c.Val()

			// Local filesystem specific settings
			case "local_path":
				if !c.NextArg() {
					return nil, c.ArgErr()
				}
				localPath = c.Val()

			default:
				return nil, c.Errf("unknown property '%s'", c.Val())
			}
		}
	}

	var uploader Uploader
	var err error

	switch storageType {

    case "s3":
		if bucket == "" || region == "" {
			return nil, c.Err("for 's3' storage, 'bucket' and 'region' must be specified")
		}
		awsCfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
		if err != nil {
			return nil, c.Errf("failed to load AWS config: %v", err)
		}
		uploader = &S3Uploader{
			Client: s3.NewFromConfig(awsCfg),
			Bucket: bucket,
			Prefix: prefix,
		}
		log.Printf("[INFO] s3dumper: configured 's3' storage for bucket '%s' in region '%s'", bucket, region)
	case "local":
		if localPath == "" {
			return nil, c.Err("for 'local' storage, 'local_path' must be specified")
		}
		uploader = &LocalUploader{Path: localPath}
		log.Printf("[INFO] s3dumper: configured 'local' storage at path '%s'", localPath)

	default:
		return nil, c.Errf("invalid storage_type '%s', must be 's3' or 'local'", storageType)
	}

	return &S3Dumper{
		Uploader:      uploader, // Assign the chosen uploader
		Buffer:        NewLogBuffer(batchSize),
		FlushInterval: flushInterval,
	}, err
}

