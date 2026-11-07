// Package rawsink batches raw platform responses into zstd-compressed JSONL
// objects in S3, so payloads can be parsed again after a parser fix or for
// a new field without collecting again (architecture §2.5: keep the raw
// responses).
//
// What it keeps, each under a prefix of its own (D-004, REQ-008 fix 17):
//   - failures (any class but OK, and responses a parser found drifted)
//     under the keep prefix, kept for good;
//   - a sample of the OK responses (Options.Sample, 1%) under the sample
//     prefix, kept for good;
//   - with Options.All (ig.capture_raw: all), the other OK responses under
//     the prefix, which a bucket rule may expire (30 days on the
//     platform's buckets).
//
// Each line names its query, doc_id, parser and parser version, so a
// re-parse knows which lines a fixed parser affects. Each kept response
// gets a reference (s3://bucket/key#line) that the events built from it
// carry as raw_ref. An upload that fails is tried again on the next flush,
// so a reference handed out is not left without its object; when uploads
// keep failing and the backlog reaches Options.MaxBacklog, new responses
// are not kept and get no reference.
package rawsink

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/minio/minio-go/v7"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/graphql"
)

// The kinds of kept lines, by prefix (metric label).
const (
	KindRaw     = "raw"
	KindFailure = "failure"
	KindSample  = "sample"
)

// DefaultMaxBacklog bounds the bytes waiting for upload.
const DefaultMaxBacklog = 256 << 20

// Options configure a Sink.
type Options struct {
	// Prefix, KeepPrefix and SamplePrefix start the keys of the OK
	// responses, the failures and the sample (s3.raw_prefix,
	// s3.raw_keep_prefix, s3.raw_sample_prefix).
	Prefix, KeepPrefix, SamplePrefix string
	// All keeps every OK response (ig.capture_raw: all); without it only
	// failures and the sample are kept.
	All bool
	// Sample is the share of OK responses kept for good under SamplePrefix
	// rather than under Prefix (ig.raw_sample); 0 keeps none.
	Sample float64
	// Versions gives a parser's version (parse.VersionOf), stamped on
	// each line; nil or 0 when the parser is unknown.
	Versions func(parser string) int
	// MaxBacklog bounds the bytes held for upload, open objects and failed
	// uploads together (DefaultMaxBacklog when 0).
	MaxBacklog int
}

// Sink implements graphql.RawSink.
type Sink struct {
	s3      *minio.Client
	bucket  string
	pod     string
	maxSize int
	o       Options
	sample  func() float64

	mu      sync.Mutex
	bufs    map[string]*batch // prefix+query -> the open object
	failed  []*batch          // closed objects whose upload failed, oldest first
	backlog int               // bytes in bufs and failed
	seq     int
}

// batch is an object being filled: its key is chosen when it opens, so a
// line's reference is known when the line is captured.
type batch struct {
	obj   string
	buf   bytes.Buffer
	lines int
}

// New writes to bucket as o says.
func New(s3 *minio.Client, bucket string, o Options) *Sink {
	pod, _ := os.Hostname()
	if o.MaxBacklog <= 0 {
		o.MaxBacklog = DefaultMaxBacklog
	}
	return &Sink{s3: s3, bucket: bucket, pod: pod, maxSize: 8 << 20, o: o, bufs: map[string]*batch{}, sample: rand.Float64}
}

// Line is one captured response (one JSONL line of a raw object).
type Line struct {
	At    time.Time `json:"at"`
	Query string    `json:"query"`
	DocID string    `json:"doc_id,omitempty"`
	// Parser and ParserVersion name the parser that read the response when
	// it was captured (0: unknown).
	Parser        string          `json:"parser,omitempty"`
	ParserVersion int             `json:"parser_version"`
	Class         string          `json:"class"`
	Vars          map[string]any  `json:"vars,omitempty"`
	Body          json.RawMessage `json:"body,omitempty"`
	Text          string          `json:"text,omitempty"`
}

// Capture keeps the response if it is a failure, in the sample, or any OK
// response with Options.All, and returns its reference (s3://bucket/key#line,
// the line counted from 1); "" when it is not kept.
func (s *Sink) Capture(ctx context.Context, r graphql.Raw) string {
	var prefix, kind string
	switch {
	case r.Class != detect.OK:
		prefix, kind = s.o.KeepPrefix, KindFailure
	case s.o.Sample > 0 && s.sample() < s.o.Sample:
		prefix, kind = s.o.SamplePrefix, KindSample
	case s.o.All:
		prefix, kind = s.o.Prefix, KindRaw
	default:
		return ""
	}
	l := Line{At: time.Now().UTC(), Query: r.Query, DocID: r.DocID, Parser: r.Parser, Class: r.Class.String(), Vars: r.Vars}
	if s.o.Versions != nil && r.Parser != "" {
		l.ParserVersion = s.o.Versions(r.Parser)
	}
	if json.Valid(r.Body) {
		l.Body = r.Body
	} else {
		l.Text = string(r.Body)
	}
	b, _ := json.Marshal(l)
	key := prefix + r.Query
	s.mu.Lock()
	if s.backlog+len(b)+1 > s.o.MaxBacklog {
		s.mu.Unlock()
		// Uploads have been failing long enough to fill the backlog: keep
		// nothing new rather than hand out a reference that may never be
		// written.
		metrics.RawNotKept.WithLabelValues("backlog").Inc()
		return ""
	}
	bt := s.bufs[key]
	if bt == nil {
		s.seq++
		bt = &batch{obj: fmt.Sprintf("%s/%s/%s-%d-%06d.jsonl.zst", key, l.At.Format("2006/01/02/15"), s.pod, l.At.UnixNano(), s.seq)}
		s.bufs[key] = bt
	}
	bt.buf.Write(b)
	bt.buf.WriteByte('\n')
	bt.lines++
	s.backlog += len(b) + 1
	ref := "s3://" + s.bucket + "/" + bt.obj + "#" + strconv.Itoa(bt.lines)
	full := bt.buf.Len() >= s.maxSize
	s.mu.Unlock()
	metrics.RawKept.WithLabelValues(kind).Inc()
	if full {
		go s.flushKey(context.WithoutCancel(ctx), key)
	}
	return ref
}

// Run flushes every interval until ctx is done, then flushes what is left,
// trying failed uploads again for up to stopFlush.
func (s *Sink) Run(ctx context.Context, every time.Duration) error {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), stopFlush)
			defer cancel()
			for s.FlushAll(fctx) > 0 && fctx.Err() == nil {
				time.Sleep(time.Second)
			}
			s.dropFailed()
			return nil
		case <-t.C:
			s.FlushAll(ctx)
		}
	}
}

// stopFlush is how long a stopping sink keeps trying its uploads.
var stopFlush = 20 * time.Second

// FlushAll uploads the failed objects again, then the open ones, and
// returns how many objects are left waiting for a later try.
func (s *Sink) FlushAll(ctx context.Context) int {
	s.mu.Lock()
	failed := s.failed
	s.failed = nil
	keys := make([]string, 0, len(s.bufs))
	for k := range s.bufs {
		keys = append(keys, k)
	}
	s.mu.Unlock()
	for _, bt := range failed {
		s.upload(ctx, bt)
	}
	for _, k := range keys {
		s.flushKey(ctx, k)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.failed)
}

func (s *Sink) flushKey(ctx context.Context, key string) {
	s.mu.Lock()
	bt := s.bufs[key]
	if bt == nil || bt.buf.Len() == 0 {
		s.mu.Unlock()
		return
	}
	delete(s.bufs, key)
	s.mu.Unlock()
	s.upload(ctx, bt)
}

// upload writes a closed object; when it fails the object waits for the
// next flush, its references still valid.
func (s *Sink) upload(ctx context.Context, bt *batch) {
	var out bytes.Buffer
	enc, _ := zstd.NewWriter(&out, zstd.WithEncoderLevel(zstd.SpeedDefault))
	_, _ = enc.Write(bt.buf.Bytes())
	_ = enc.Close()
	_, err := s.s3.PutObject(ctx, s.bucket, bt.obj, &out, int64(out.Len()), minio.PutObjectOptions{ContentType: "application/zstd"})
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		metrics.RawUploadFailures.Inc()
		slog.Warn("rawsink: upload failed; trying again on the next flush", "object", bt.obj, "lines", bt.lines, "err", err)
		s.failed = append(s.failed, bt)
	} else {
		s.backlog -= bt.buf.Len()
	}
	metrics.RawBacklog.Set(float64(s.backlog))
}

// dropFailed gives up on the uploads a stopping sink could not make: their
// references point at objects that will not exist, so each is logged.
func (s *Sink) dropFailed() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, bt := range s.failed {
		metrics.RawNotKept.WithLabelValues("lost").Add(float64(bt.lines))
		slog.Error("rawsink: object never written; its references dangle", "object", bt.obj, "lines", bt.lines)
		s.backlog -= bt.buf.Len()
	}
	s.failed = nil
	metrics.RawBacklog.Set(float64(s.backlog))
}
