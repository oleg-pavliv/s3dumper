// coredns-s3dumper/logentry.go
package s3dumper

import (
	"net"
	"strings"
	"time"

	"github.com/miekg/dns"
)

// LogEntry represents a single DNS request/response log, structured for Parquet.
type LogEntry struct {
	// CORRECTED TAG SYNTAX BELOW
	Timestamp int64    `parquet:"name=timestamp, type=INT64, logicaltype=TIMESTAMP, logicaltype.isadjustedtoutc=true, logicaltype.unit=MILLIS"`
	ClientIP  string   `parquet:"name=client_ip, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Proto     string   `parquet:"name=protocol, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Name      string   `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Type      string   `parquet:"name=type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Class     string   `parquet:"name=class, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Rcode     string   `parquet:"name=rcode, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Size      int32    `parquet:"name=size, type=INT32"`
	Duration  float64  `parquet:"name=duration_sec, type=DOUBLE"`
	Answers   []string `parquet:"name=answers, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY, repetitiontype=REPEATED"`
}

// ... NewLogEntry function remains exactly the same ...
func NewLogEntry(r *dns.Msg, clientIP net.Addr, proto string, start time.Time, resp *dns.Msg) *LogEntry {
	entry := &LogEntry{
		Timestamp: start.UnixMilli(),
		ClientIP:  clientIP.String(),
		Proto:     proto,
		Name:      r.Question[0].Name,
		Type:      dns.TypeToString[r.Question[0].Qtype],
		Class:     dns.ClassToString[r.Question[0].Qclass],
		Duration:  time.Since(start).Seconds(),
	}

	if resp != nil {
		entry.Rcode = dns.RcodeToString[resp.Rcode]
		entry.Size = int32(resp.Len())
		for _, rr := range resp.Answer {
			sanitizedRR := strings.ReplaceAll(rr.String(), "\t", " ")
			entry.Answers = append(entry.Answers, sanitizedRR)
		}
	} else {
		entry.Rcode = "SERVFAIL"
	}

	return entry
}

/*
// coredns-s3dumper/logentry.go
package s3dumper

import (
	"net"
	"strings"
	"time"

	"github.com/miekg/dns"
)

// LogEntry represents a single DNS request/response log.
type LogEntry struct {
	Timestamp time.Time `json:"timestamp"`
	ClientIP  string    `json:"client_ip"`
	Proto     string    `json:"protocol"`
	Name      string    `json:"name"`
	Type      string    `json:"type"`
	Class     string    `json:"class"`
	Rcode     string    `json:"rcode"`
	Size      int       `json:"size"`
	Duration  float64   `json:"duration_sec"`
	Answers   []string  `json:"answers,omitempty"`
}

// NewLogEntry creates a LogEntry from a request and response.
func NewLogEntry(r *dns.Msg, clientIP net.Addr, proto string, start time.Time, resp *dns.Msg) *LogEntry {
	entry := &LogEntry{
		Timestamp: start,
		ClientIP:  clientIP.String(),
		Proto:     proto,
		Name:      r.Question[0].Name,
		Type:      dns.TypeToString[r.Question[0].Qtype],
		Class:     dns.ClassToString[r.Question[0].Qclass],
		Duration:  time.Since(start).Seconds(),
	}

	if resp != nil {
		entry.Rcode = dns.RcodeToString[resp.Rcode]
		entry.Size = resp.Len()
		for _, rr := range resp.Answer {
			// Sanitize the answer to prevent log injection or formatting issues.
			sanitizedRR := strings.ReplaceAll(rr.String(), "\t", " ")
			entry.Answers = append(entry.Answers, sanitizedRR)
		}
	} else {
		// In case of error where we don't get a response message
		entry.Rcode = "SERVFAIL"
	}

	return entry
}
*/
