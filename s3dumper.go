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
	Next          plugin.Handler
	Uploader      Uploader // <--- CHANGED: Use the interface
	Buffer        *LogBuffer
	FlushInterval time.Duration

	stop chan struct{}
}

// ... (The rest of s3dumper.go remains EXACTLY the same) ...

// Name implements the plugin.Handler interface.
func (s *S3Dumper) Name() string { return "s3dumper" }

// ServeDNS implements the plugin.Handler interface.
func (s *S3Dumper) ServeDNS(ctx context.Context, w dns.ResponseWriter, r *dns.Msg) (int, error) {
	rw := dnstest.NewRecorder(w)
	start := time.Now()

	status, err := plugin.NextOrFailure(s.Name(), s.Next, ctx, rw, r)

	if r.Question != nil && len(r.Question) > 0 {
		reqState := request.Request{W: w, Req: r}
		
		resp := rw.Msg
		if resp == nil {
			resp = new(dns.Msg)
			resp.SetRcode(r, dns.RcodeServerFailure)
			if status != 0 {
				resp.Rcode = status
			}
		}

		entry := NewLogEntry(r, reqState.W.RemoteAddr(), reqState.Proto(), start, resp)
		toFlush := s.Buffer.Add(entry)
		if toFlush != nil {
			go s.Uploader.Upload(toFlush)
		}
	}

	return status, err
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

