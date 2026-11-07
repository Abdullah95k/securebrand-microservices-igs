package rawsink

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/klauspost/compress/zstd"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	dto "github.com/prometheus/client_model/go"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/detect"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/graphql"
)

// fakeS3 is an in-memory S3 whose uploads fail while down is set.
type fakeS3 struct {
	*minio.Client
	down atomic.Bool
}

func newS3(t *testing.T) *fakeS3 {
	t.Helper()
	f := &fakeS3{}
	fake := gofakes3.New(s3mem.New()).Server()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && f.down.Load() && strings.Count(strings.Trim(r.URL.Path, "/"), "/") > 0 {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		fake.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	u, _ := url.Parse(srv.URL)
	c, err := minio.New(u.Host, &minio.Options{Creds: credentials.NewStaticV4("k", "s", ""), Region: "us-east-1",
		MaxRetries: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.MakeBucket(context.Background(), "raw", minio.MakeBucketOptions{}); err != nil {
		t.Fatal(err)
	}
	f.Client = c
	return f
}

// line reads the line a reference names, and whether the object exists.
func line(t *testing.T, s3 *minio.Client, ref string) (Line, bool) {
	t.Helper()
	rest, ok := strings.CutPrefix(ref, "s3://raw/")
	key, lineStr, ok2 := strings.Cut(rest, "#")
	n, _ := strconv.Atoi(lineStr)
	if !ok || !ok2 || n < 1 {
		t.Fatalf("reference %q", ref)
	}
	obj, err := s3.GetObject(context.Background(), "raw", key, minio.GetObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = obj.Close() }()
	if _, err := obj.Stat(); err != nil {
		return Line{}, false
	}
	zr, err := zstd.NewReader(obj)
	if err != nil {
		t.Fatal(err)
	}
	defer zr.Close()
	sc := bufio.NewScanner(zr)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	var got Line
	for i := 1; sc.Scan(); i++ {
		if i == n {
			if err := json.Unmarshal(sc.Bytes(), &got); err != nil {
				t.Fatal(err)
			}
		}
	}
	return got, true
}

func versions(parser string) int {
	if parser == "comments" {
		return 3
	}
	return 0
}

// Every kept response gets a reference (s3://bucket/key#line) that names,
// once flushed, the object and the line holding exactly it, with its
// query, doc_id, parser and parser version. OK responses go under the
// prefix, failures under the keep prefix: igs's own bucket's raw/ and
// raw-failures/, or the platform's shared bucket's igs/30d/ and
// igs/keep/failures/.
func TestCaptureReferencesTheStoredLine(t *testing.T) {
	for _, p := range [][2]string{{"raw/", "raw-failures/"}, {"igs/30d/", "igs/keep/failures/"}} {
		t.Run(p[0], func(t *testing.T) { testCapture(t, p[0], p[1]) })
	}
}

func testCapture(t *testing.T, prefix, keepPrefix string) {
	s3 := newS3(t)
	ctx := context.Background()
	sink := New(s3.Client, "raw", Options{Prefix: prefix, KeepPrefix: keepPrefix, SamplePrefix: "sample/", All: true,
		Versions: versions})
	refs := map[string]string{}
	for i, body := range []string{`{"data":{"n":1}}`, `{"data":{"n":2}}`, `<html>page</html>`} {
		r := graphql.Raw{Query: "comments", DocID: "123", Parser: "comments", Class: detect.OK, Body: []byte(body), Vars: map[string]any{"i": i}}
		if i == 2 {
			r = graphql.Raw{Query: "post_page", Parser: "post", Class: detect.OK, Body: []byte(body)}
		}
		refs[sink.Capture(ctx, r)] = body
	}
	failed := sink.Capture(ctx, graphql.Raw{Query: "comments", Parser: "comments", Class: detect.RateLimited, Body: []byte(`{"message":"wait"}`)})
	if n := sink.FlushAll(ctx); n != 0 {
		t.Fatalf("%d objects left after a flush", n)
	}
	if !strings.HasPrefix(failed, "s3://raw/"+keepPrefix+"comments/") {
		t.Fatalf("a failed response's reference %q is not under %s", failed, keepPrefix)
	}
	if l, ok := line(t, s3.Client, failed); !ok || l.Class != "rate_limited" || l.ParserVersion != 3 {
		t.Fatalf("the failure's line: %+v %v", l, ok)
	}
	for ref, body := range refs {
		if !strings.HasPrefix(ref, "s3://raw/"+prefix) {
			t.Fatalf("an OK response's reference %q is not under %s", ref, prefix)
		}
		got, ok := line(t, s3.Client, ref)
		if !ok || (string(got.Body) != body && got.Text != body) {
			t.Fatalf("%s holds %s / %q, want %s", ref, got.Body, got.Text, body)
		}
		if got.Query == "comments" && (got.DocID != "123" || got.Parser != "comments" || got.ParserVersion != 3) {
			t.Fatalf("%s: doc_id %q parser %q version %d", ref, got.DocID, got.Parser, got.ParserVersion)
		}
		if got.Query == "post_page" && (got.Parser != "post" || got.ParserVersion != 0) {
			t.Fatalf("%s: parser %q version %d", ref, got.Parser, got.ParserVersion)
		}
	}
}

// A share of the OK responses (ig.raw_sample) is kept for good under the
// sample prefix instead of the prefix: here every second one. Without
// capture_raw all, the other OK responses are not kept and get no
// reference, while failures and the sample still are.
func TestSample(t *testing.T) {
	s3 := newS3(t)
	ctx := context.Background()
	for _, all := range []bool{true, false} {
		sink := New(s3.Client, "raw", Options{Prefix: "igs/30d/", KeepPrefix: "igs/keep/failures/", SamplePrefix: "igs/keep/sample/",
			All: all, Sample: 0.5, Versions: versions})
		var draws int
		sink.sample = func() float64 { draws++; return float64(draws%2) * 0.9 } // 0.9, 0, 0.9, 0, ...
		var raw, sampled, none int
		for range 10 {
			ref := sink.Capture(ctx, graphql.Raw{Query: "comments", Parser: "comments", Class: detect.OK, Body: []byte(`{"data":{}}`)})
			switch {
			case strings.HasPrefix(ref, "s3://raw/igs/keep/sample/comments/"):
				sampled++
			case strings.HasPrefix(ref, "s3://raw/igs/30d/comments/"):
				raw++
			case ref == "":
				none++
			default:
				t.Fatalf("reference %q", ref)
			}
		}
		failed := sink.Capture(ctx, graphql.Raw{Query: "comments", Class: detect.LoginWall, Body: []byte(`<html>`)})
		if sampled != 5 || (all && (raw != 5 || none != 0)) || (!all && (raw != 0 || none != 5)) {
			t.Fatalf("all %v: %d sampled, %d under raw, %d not kept", all, sampled, raw, none)
		}
		if !strings.HasPrefix(failed, "s3://raw/igs/keep/failures/comments/") {
			t.Fatalf("all %v: the failure's reference %q", all, failed)
		}
		sink.FlushAll(ctx)
	}
}

func counterValue(c interface{ Write(*dto.Metric) error }) float64 {
	var m dto.Metric
	_ = c.Write(&m)
	return m.GetCounter().GetValue()
}

// An upload that fails is tried again on the next flush, so no reference
// handed out is left without its object: while S3 refuses uploads the
// objects wait, and once it is back they are written with every line.
// When the waiting bytes reach the backlog limit, new responses are not
// kept and get no reference. A stopping sink that still cannot upload
// logs the objects lost.
func TestUploadsTriedAgain(t *testing.T) {
	s3 := newS3(t)
	ctx := context.Background()
	sink := New(s3.Client, "raw", Options{Prefix: "raw/", KeepPrefix: "keep/", SamplePrefix: "sample/", All: true})
	body := []byte(`{"data":{"text":"` + strings.Repeat("x", 900) + `"}}`)
	capture := func() string {
		return sink.Capture(ctx, graphql.Raw{Query: "comments", Class: detect.OK, Body: body})
	}
	s3.down.Store(true)
	first := []string{capture()}
	sink.o.MaxBacklog = 4*sink.backlog + sink.backlog/2 // room for four lines
	first = append(first, capture())
	failures := counterValue(metrics.RawUploadFailures)
	if n := sink.FlushAll(ctx); n != 1 {
		t.Fatalf("%d objects waiting after a failed upload, want 1", n)
	}
	if counterValue(metrics.RawUploadFailures) <= failures {
		t.Fatal("the failed upload was not counted")
	}
	// More lines while S3 is down: a new object; past the backlog limit,
	// nothing more is kept.
	second := []string{capture(), capture()}
	backlog := counterValue(metrics.RawNotKept.WithLabelValues("backlog"))
	if ref := capture(); ref != "" {
		t.Fatalf("a response past the backlog limit got reference %q", ref)
	}
	if counterValue(metrics.RawNotKept.WithLabelValues("backlog")) != backlog+1 {
		t.Fatal("the response not kept was not counted")
	}
	if n := sink.FlushAll(ctx); n != 2 {
		t.Fatalf("%d objects waiting, want 2", n)
	}
	for _, ref := range append(first, second...) {
		if _, ok := line(t, s3.Client, ref); ok {
			t.Fatalf("%s exists while uploads fail", ref)
		}
	}
	// S3 is back: every reference handed out resolves.
	s3.down.Store(false)
	if n := sink.FlushAll(ctx); n != 0 {
		t.Fatalf("%d objects still waiting", n)
	}
	for _, ref := range append(first, second...) {
		if l, ok := line(t, s3.Client, ref); !ok || !strings.Contains(string(l.Body), "xxx") {
			t.Fatalf("%s: not written after S3 came back", ref)
		}
	}
	if ref := capture(); ref == "" {
		t.Fatal("responses are kept again once the backlog is written")
	}

	// A sink stopping while S3 refuses: it tries for a while, then logs and
	// counts the lines lost.
	s3.down.Store(true)
	stopFlush = 2 * time.Second
	t.Cleanup(func() { stopFlush = 20 * time.Second })
	lost := counterValue(metrics.RawNotKept.WithLabelValues("lost"))
	rctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { _ = sink.Run(rctx, time.Hour); close(done) }()
	cancel()
	select {
	case <-done:
	case <-time.After(stopFlush + 10*time.Second):
		t.Fatal("the sink did not stop")
	}
	if got := counterValue(metrics.RawNotKept.WithLabelValues("lost")) - lost; got != 1 {
		t.Fatalf("%v lines counted lost, want 1", got)
	}
}
