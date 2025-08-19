// coredns-s3dumper/s3dumper.go
package s3dumper

import (
	"context"
	"log"
	"time"

	"github.com/coredns/coredns/plugin"
	"github.com/coredns/coredns/plugin/pkg/dnstest"
	"github.com/coredns/coredns/request"

	"github.com/miekg/dns"
)

// Uploader is the interface for uploading log entries.
// This allows for different backends like S3 or local filesystem.
type Uploader interface {
	Upload(entries []*LogEntry)
}

// S3Dumper is the main plugin struct.
type S3Dumper struct {
    Next        plugin.Handler

    // Low-priority logging pipeline
    queue       chan *LogEntry
    stop        chan struct{}
    workers     int

    // Batching / backpressure
    batchSize   int
    flushEvery  time.Duration
    dropThresh  int // percentage (0..100); start shedding when queue >= this%

    // Existing components
    Uploader    *Uploader
}

func (s *S3Dumper) Init() {
    if s.workers == 0 {
        s.workers = 4
    }
    if s.batchSize == 0 {
        s.batchSize = 1000
    }
    if s.flushEvery == 0 {
        s.flushEvery = 5 * time.Second
    }
    if s.dropThresh == 0 {
        s.dropThresh = 90 // start shedding when >=90% full
    }
    if s.queue == nil {
        s.queue = make(chan *LogEntry, 50_000)
    }
    s.stop = make(chan struct{})

    for i := 0; i < s.workers; i++ {
        go s.worker()
    }
}

// Name implements the plugin.Handler interface.
func (s *S3Dumper) Name() string { return "s3dumper" }


// ServeDNS implements the plugin.Handler interface.
func (s *S3Dumper) ServeDNS(ctx context.Context, w dns.ResponseWriter, r *dns.Msg) (int, error) {
    // If you only need the question + proto + rcode, you can avoid a recorder.
    // If you DO need the final response, keep the recorder:
    rw := dnstest.NewRecorder(w) // ok, but note it adds overhead

    start := time.Now()
    status, err := plugin.NextOrFailure(s.Name(), s.Next, ctx, rw, r)

    if r.Question != nil && len(r.Question) > 0 {
        reqState := request.Request{W: w, Req: r}

        resp := rw.Msg
        if resp == nil {
            // Synthesize a minimal response snapshot
            resp = new(dns.Msg)
            resp.SetRcode(r, dns.RcodeServerFailure)
            if status != 0 {
                resp.Rcode = status
            }
        }

        entry := NewLogEntry(r, reqState.W.RemoteAddr(), reqState.Proto(), start, resp)

        // Fast-path: adaptive shedding if queue close to full
        // (avoid atomic/locks by sampling len(queue))
        qlen := len(s.queue)
        if cap(s.queue) > 0 && qlen*100 >= cap(s.queue)*s.dropThresh {
            // Overloaded → skip logging altogether
            return status, err
        }

        // Non-blocking enqueue: drop if full
        select {
        case s.queue <- entry:
            // enqueued
        default:
            // queue full → drop silently, never block DNS
        }
    }

    return status, err
}

func (s *S3Dumper) worker() {
    ticker := time.NewTicker(s.flushEvery)
    defer ticker.Stop()

    batch := make([]*LogEntry, 0, s.batchSize)

    flush := func() {
        if len(batch) == 0 {
            return
        }
        // Upload synchronously inside the worker
        // Make a copy to avoid holding onto large backing arrays
        toUpload := make([]*LogEntry, len(batch))
        copy(toUpload, batch)
        // Reset the batch quickly
        batch = batch[:0]

        // Best effort; errors are logged, not propagated to ServeDNS
        if err := s.Uploader.Upload(toUpload); err != nil {
            // TODO: add your logger
            // log.Printf("[s3dumper] upload failed: %v", err)
        }
    }

    for {
        select {
        case <-s.stop:
            // drain
            for {
                select {
                case e := <-s.queue:
                    batch = append(batch, e)
                    if len(batch) >= s.batchSize {
                        flush()
                    }
                default:
                    flush()
                    return
                }
            }
        case e := <-s.queue:
            batch = append(batch, e)
            if len(batch) >= s.batchSize {
                flush()
            }
        case <-ticker.C:
            flush()
        }
    }
}




// Start runs the background ticker for flushing logs.
func (s *S3Dumper) Start() {
	s.stop = make(chan struct{})
	ticker := time.NewTicker(s.FlushInterval)

	go func() {
		for {
			select {
			case <-ticker.C:
				toFlush := s.Buffer.Flush()
				if toFlush != nil {
					go s.Uploader.Upload(toFlush)
				}
			case <-s.stop:
				ticker.Stop()
				return
			}
		}
	}()
	log.Printf("[INFO] s3dumper: started with flush interval %v", s.FlushInterval)
}

// Shutdown gracefully stops the plugin.
func (s *S3Dumper) Shutdown() error {
	close(s.stop)

	toFlush := s.Buffer.Flush()
	if toFlush != nil {
		log.Printf("[INFO] s3dumper: performing final flush of %d log entries on shutdown", len(toFlush))
		s.Uploader.Upload(toFlush)
	}
	log.Printf("[INFO] s3dumper: successfully shut down", s.FlushInterval)
	return nil
}

func (s *S3Dumper) OnShutdown() error {
    close(s.stop)
    // Optionally wait a short grace period for workers to finish
    // or use a sync.WaitGroup to join workers.
    return nil
}


