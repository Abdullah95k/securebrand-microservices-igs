package rawsink

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/klauspost/compress/zstd"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/detect"
)

// Every captured response gets a reference (s3://bucket/key#line) that
// names, once flushed, the object and the line holding exactly it. OK
// responses go under the prefix, failures under the keep prefix: igs's own
// bucket's raw/ and raw-failures/, or the platform's shared bucket's
// igs/30d/ and igs/keep/.
func TestCaptureReferencesTheStoredLine(t *testing.T) {
	for _, p := range [][2]string{{"raw/", "raw-failures/"}, {"igs/30d/", "igs/keep/"}} {
		t.Run(p[0], func(t *testing.T) { testCapture(t, p[0], p[1]) })
	}
}

func testCapture(t *testing.T, prefix, keepPrefix string) {
	srv := httptest.NewServer(gofakes3.New(s3mem.New()).Server())
	t.Cleanup(srv.Close)
	u, _ := url.Parse(srv.URL)
	s3, err := minio.New(u.Host, &minio.Options{Creds: credentials.NewStaticV4("k", "s", ""), Region: "us-east-1"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := s3.MakeBucket(ctx, "raw", minio.MakeBucketOptions{}); err != nil {
		t.Fatal(err)
	}
	sink := New(s3, "raw", prefix, keepPrefix)
	refs := map[string]string{}
	for i, body := range []string{`{"data":{"n":1}}`, `{"data":{"n":2}}`, `<html>page</html>`} {
		q := "comments"
		if i == 2 {
			q = "post_page"
		}
		refs[sink.Capture(ctx, q, detect.OK, []byte(body), map[string]any{"i": i})] = body
	}
	failed := sink.Capture(ctx, "comments", detect.RateLimited, []byte(`{"message":"wait"}`), nil)
	sink.FlushAll(ctx)
	if !strings.HasPrefix(failed, "s3://raw/"+keepPrefix+"comments/") {
		t.Fatalf("a failed response's reference %q is not under %s", failed, keepPrefix)
	}
	for ref, body := range refs {
		rest, ok := strings.CutPrefix(ref, "s3://raw/")
		key, lineStr, ok2 := strings.Cut(rest, "#")
		line, _ := strconv.Atoi(lineStr)
		if !ok || !ok2 || line < 1 || !strings.HasPrefix(key, prefix) {
			t.Fatalf("reference %q", ref)
		}
		obj, err := s3.GetObject(ctx, "raw", key, minio.GetObjectOptions{})
		if err != nil {
			t.Fatal(err)
		}
		zr, err := zstd.NewReader(obj)
		if err != nil {
			t.Fatal(err)
		}
		sc := bufio.NewScanner(zr)
		sc.Buffer(make([]byte, 1<<20), 1<<20)
		var got Line
		for n := 1; sc.Scan(); n++ {
			if n == line {
				_ = json.Unmarshal(sc.Bytes(), &got)
			}
		}
		zr.Close()
		if string(got.Body) != body && got.Text != body {
			t.Fatalf("%s holds %s / %q, want %s", ref, got.Body, got.Text, body)
		}
	}
}
