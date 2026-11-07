package e2e

import (
	"bufio"
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/minio/minio-go/v7"
	"google.golang.org/protobuf/proto"

	igv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/ig/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/rawsink"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/parse"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/topics"
)

// With ig.capture_raw all, every response the crawl read is kept in the raw
// bucket: the GraphQL answers (profile, grid, reels, highlights, comments)
// and the post pages, so a parser fix can be applied to past data. A share
// of them (ig.raw_sample, here a quarter) is kept for good under the
// sample prefix instead (D-004, REQ-008 fix 17). Every line names its
// query, its parser and that parser's version, and the GraphQL answers
// their doc_id. Every post event's raw_ref names a line that exists and
// holds a post page.
func TestRawResponsesKept(t *testing.T) {
	sp := spec()
	sp.Profiles = 1
	s := Start(t, Options{Spec: sp, RawCapture: true, Config: func(c *config.Config) { c.IG.RawSample = 0.25 }})
	p := s.World.Profiles()[0]
	s.Wait(t, 2*time.Minute, s.Crawl(t, p.Username, full, s.Limits(), nil))
	s.stop("crawler") // the sink flushes on stop
	ctx := context.Background()
	bucket := s.Cfg.S3.RawBucket

	read := func(key string) []rawsink.Line {
		t.Helper()
		obj, err := s.S3.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = obj.Close() }()
		zr, err := zstd.NewReader(obj)
		if err != nil {
			t.Fatal(err)
		}
		defer zr.Close()
		sc := bufio.NewScanner(zr)
		sc.Buffer(make([]byte, 4<<20), 4<<20)
		var lines []rawsink.Line
		for sc.Scan() {
			var l rawsink.Line
			if err := json.Unmarshal(sc.Bytes(), &l); err != nil {
				t.Fatal(err)
			}
			lines = append(lines, l)
		}
		if err := sc.Err(); err != nil {
			t.Fatalf("%s: %v", key, err)
		}
		return lines
	}
	versions := map[string]int{"profile": parse.ProfileVersion, "grid": parse.GridVersion, "comments": parse.CommentsVersion,
		"reels": parse.ReelsVersion, "highlights": parse.HighlightsVersion, parse.ParserPost: parse.PostVersion}
	seen := map[string]map[string]int{} // prefix -> query -> lines
	objects := map[string][]rawsink.Line{}
	for _, prefix := range []string{s.Cfg.S3.RawPrefix, s.Cfg.S3.RawSamplePrefix} {
		seen[prefix] = map[string]int{}
		for obj := range s.S3.ListObjects(ctx, bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
			if obj.Err != nil {
				t.Fatal(obj.Err)
			}
			lines := read(obj.Key)
			objects[obj.Key] = lines
			for _, l := range lines {
				seen[prefix][l.Query]++
				want, known := versions[l.Parser]
				if !known || l.ParserVersion != want || l.ParserVersion == 0 {
					t.Errorf("%s: a %s line names parser %q version %d", obj.Key, l.Query, l.Parser, l.ParserVersion)
				}
				if l.Query != "post_page" && l.DocID == "" {
					t.Errorf("%s: a %s line has no doc_id", obj.Key, l.Query)
				}
			}
		}
	}
	for _, q := range []string{"profile", "comments", "post_page", "reels"} {
		if seen[s.Cfg.S3.RawPrefix][q]+seen[s.Cfg.S3.RawSamplePrefix][q] == 0 {
			t.Errorf("no raw %s responses kept: %v", q, seen)
		}
	}
	var sampled, kept int
	for _, n := range seen[s.Cfg.S3.RawSamplePrefix] {
		sampled += n
	}
	for _, n := range seen[s.Cfg.S3.RawPrefix] {
		kept += n
	}
	if sampled == 0 || kept == 0 || sampled > kept {
		t.Errorf("%d lines in the sample, %d under the prefix: want about a quarter sampled", sampled, kept)
	}

	// Every post event's raw_ref resolves to its post page.
	var refs int
	for _, r := range s.ReadTopic(t, topics.Post, 3*time.Second) {
		var post igv1.Post
		if err := proto.Unmarshal(r.Value, &post); err != nil {
			t.Fatal(err)
		}
		ref := post.GetObs().GetRawRef()
		if ref == "" {
			continue
		}
		refs++
		rest, ok := strings.CutPrefix(ref, "s3://"+bucket+"/")
		key, n, ok2 := strings.Cut(rest, "#")
		i, _ := strconv.Atoi(n)
		lines, found := objects[key]
		if !ok || !ok2 || !found || i < 1 || i > len(lines) {
			t.Errorf("post %d: raw_ref %s names no stored line", post.GetMediaPk(), ref)
			continue
		}
		if l := lines[i-1]; l.Query != "post_page" || l.Text == "" {
			t.Errorf("post %d: raw_ref %s holds a %s line", post.GetMediaPk(), ref, l.Query)
		}
	}
	if refs == 0 {
		t.Error("no post event carried a raw_ref")
	}
}
