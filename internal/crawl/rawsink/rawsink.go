// Package rawsink batches raw platform responses into zstd-compressed JSONL
// objects in S3, so payloads can be parsed again after a parser fix or for
// a new field without collecting again (architecture §2.5: keep the raw
// responses). Failures go under the keep prefix (raw-failures/ by default,
// kept forever), everything else under the other (raw/; one a bucket rule
// may expire).
// Each captured response gets a reference (s3://bucket/key#line) that the
// events built from it carry as raw_ref.
package rawsink

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/minio/minio-go/v7"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/detect"
)

// Sink implements graphql.RawSink.
type Sink struct {
	s3      *minio.Client
	bucket  string
	pod     string
	maxSize int
	// prefix and keepPrefix start the keys of OK responses and failures.
	prefix, keepPrefix string

	mu   sync.Mutex
	bufs map[string]*batch // prefix/query -> the open object
	seq  int
}

// batch is an object being filled: its key is chosen when it opens, so a
// line's reference is known when the line is captured.
type batch struct {
	obj   string
	buf   bytes.Buffer
	lines int
}

// New writes to bucket: OK responses under prefix, failures under
// keepPrefix (s3.raw_prefix, s3.raw_keep_prefix).
func New(s3 *minio.Client, bucket, prefix, keepPrefix string) *Sink {
	pod, _ := os.Hostname()
	return &Sink{s3: s3, bucket: bucket, pod: pod, maxSize: 8 << 20, bufs: map[string]*batch{}, prefix: prefix, keepPrefix: keepPrefix}
}

// Line is one captured response (one JSONL line of a raw object).
type Line struct {
	At    time.Time       `json:"at"`
	Query string          `json:"query"`
	Class string          `json:"class"`
	Vars  map[string]any  `json:"vars,omitempty"`
	Body  json.RawMessage `json:"body,omitempty"`
	Text  string          `json:"text,omitempty"`
}

// Capture buffers one response and returns its reference
// (s3://bucket/key#line, the line counted from 1).
func (s *Sink) Capture(ctx context.Context, query string, cls detect.Class, body []byte, vars map[string]any) string {
	prefix := s.prefix
	if cls != detect.OK {
		prefix = s.keepPrefix
	}
	l := Line{At: time.Now().UTC(), Query: query, Class: cls.String(), Vars: vars}
	if json.Valid(body) {
		l.Body = body
	} else {
		l.Text = string(body)
	}
	b, _ := json.Marshal(l)
	key := prefix + query
	s.mu.Lock()
	bt := s.bufs[key]
	if bt == nil {
		s.seq++
		bt = &batch{obj: fmt.Sprintf("%s/%s/%s-%d-%06d.jsonl.zst", key, l.At.Format("2006/01/02/15"), s.pod, l.At.UnixNano(), s.seq)}
		s.bufs[key] = bt
	}
	bt.buf.Write(b)
	bt.buf.WriteByte('\n')
	bt.lines++
	ref := "s3://" + s.bucket + "/" + bt.obj + "#" + strconv.Itoa(bt.lines)
	full := bt.buf.Len() >= s.maxSize
	s.mu.Unlock()
	if full {
		go s.flushKey(context.WithoutCancel(ctx), key)
	}
	return ref
}

// Run flushes buffers every interval until ctx is done.
func (s *Sink) Run(ctx context.Context, every time.Duration) error {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			s.FlushAll(context.Background())
			return nil
		case <-t.C:
			s.FlushAll(ctx)
		}
	}
}

func (s *Sink) FlushAll(ctx context.Context) {
	s.mu.Lock()
	keys := make([]string, 0, len(s.bufs))
	for k := range s.bufs {
		keys = append(keys, k)
	}
	s.mu.Unlock()
	for _, k := range keys {
		s.flushKey(ctx, k)
	}
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

	var out bytes.Buffer
	enc, _ := zstd.NewWriter(&out, zstd.WithEncoderLevel(zstd.SpeedDefault))
	_, _ = enc.Write(bt.buf.Bytes())
	_ = enc.Close()
	obj := bt.obj
	_, err := s.s3.PutObject(ctx, s.bucket, obj, &out, int64(out.Len()), minio.PutObjectOptions{ContentType: "application/zstd"})
	if err != nil {
		slog.Warn("rawsink: upload failed", "object", obj, "err", err)
	}
}
