package e2e

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/common/v1"
	igstaskv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/igs/task/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/httpapi"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/api"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/topics"
)

// A job that asks for media files gets each post's renditions downloaded
// into object storage (ready, with a size and hash); once the analysis
// service acknowledges them, the janitor deletes the objects.
func TestMediaPipeline(t *testing.T) {
	sp := spec()
	sp.Profiles, sp.PostsPerProfile = 1, 12
	s := Start(t, Options{Spec: sp})
	s.StartMedia(t)
	ctx := context.Background()
	lim := s.Limits()
	lim.IncludeMedia, lim.VideoPolicy = true, igstaskv1.VideoPolicy_VIDEO_POLICY_SD
	p := s.World.Profiles()[0]
	job := s.Crawl(t, p.Username, full, lim, nil)
	if st := s.Wait(t, 3*time.Minute, job); st[job].Counters["media_queued"] == 0 {
		t.Fatalf("no media queued: %v", st[job].Counters)
	}
	var ready int
	deadline := time.Now().Add(time.Minute)
	for {
		if err := s.DB.QueryRow(ctx, `SELECT count(*) FROM media WHERE status = 'ready' AND user_pk = $1`, p.PK).Scan(&ready); err != nil {
			t.Fatal(err)
		}
		var pending int
		_ = s.DB.QueryRow(ctx, `SELECT count(*) FROM media WHERE status = 'discovered' AND user_pk = $1 AND owner_type = 'post'
			AND variant IN ('original','sd','thumbnail')`, p.PK).Scan(&pending)
		if ready > 0 && pending == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("media not downloaded: %d ready, %d discovered", ready, pending)
		}
		time.Sleep(200 * time.Millisecond)
	}
	rows, err := s.DB.Query(ctx, `SELECT media_id::text, s3_bucket, s3_key, size_bytes, sha256 FROM media WHERE status = 'ready' AND user_pk = $1`, p.PK)
	if err != nil {
		t.Fatal(err)
	}
	type obj struct {
		id, bucket, key string
		size            int64
		sha             []byte
	}
	var objs []obj
	for rows.Next() {
		var o obj
		if err := rows.Scan(&o.id, &o.bucket, &o.key, &o.size, &o.sha); err != nil {
			t.Fatal(err)
		}
		objs = append(objs, o)
	}
	rows.Close()
	for _, o := range objs {
		if o.size == 0 || len(o.sha) != 32 {
			t.Fatalf("media %s: size %d sha %x", o.id, o.size, o.sha)
		}
		if _, err := s.S3.StatObject(ctx, o.bucket, o.key, minio.StatObjectOptions{}); err != nil {
			t.Fatalf("object %s: %v", o.key, err)
		}
	}
	// The analysis service acknowledges every object.
	b := s.Producer.NewBatch()
	for _, o := range objs {
		if err := b.AddProto(ctx, topics.MediaProcessed, o.id, &commonv1.MediaProcessed{EventId: o.id + ":ack", MediaId: o.id,
			Consumer: s.Cfg.Media.RequiredConsumers[0], Status: commonv1.ProcessStatus_PROCESS_STATUS_PROCESSED,
			ProcessedAt: timestamppb.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(time.Minute)
	for {
		var left int
		_ = s.DB.QueryRow(ctx, `SELECT count(*) FROM media WHERE status = 'ready' AND user_pk = $1`, p.PK).Scan(&left)
		if left == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d objects still ready after the acks", left)
		}
		time.Sleep(200 * time.Millisecond)
	}
	for _, o := range objs {
		if _, err := s.S3.StatObject(ctx, o.bucket, o.key, minio.StatObjectOptions{}); err == nil {
			t.Fatalf("object %s still stored", o.key)
		}
	}
}

// Erasing a commenter redacts their comments, and a crawl after it stores
// their new sightings redacted; erasing a profile deletes everything read
// from it, and a crawl after it stores nothing.
func TestErasure(t *testing.T) {
	sp := spec()
	sp.Profiles = 2
	s := Start(t, Options{Spec: sp})
	s.StartEraser()
	ctx := context.Background()
	admin := s.Key(t, httpapi.ScopeAdmin)
	lim := s.Limits()
	p, other := s.World.Profiles()[0], s.World.Profiles()[1]
	s.Wait(t, 3*time.Minute, s.Crawl(t, p.Username, full, lim, nil), s.Crawl(t, other.Username, full, lim, nil))

	// A commenter with stored comments.
	var name string
	var authorPK int64
	if err := s.DB.QueryRow(ctx, `SELECT a.username, a.author_pk FROM comment_authors a JOIN comments c ON c.author_key = a.author_key
		WHERE c.owner_pk = $1 GROUP BY a.username, a.author_pk ORDER BY count(*) DESC LIMIT 1`, p.PK).Scan(&name, &authorPK); err != nil {
		t.Fatal(err)
	}
	var e api.Erasure
	if code := s.Do(t, admin, "POST", "/v1/erasure-requests", map[string]any{"subject_type": "author", "username": name}, &e); code != http.StatusAccepted {
		t.Fatalf("author erasure: %d", code)
	}
	waitErasure(t, s, admin, e.ID.String())
	var visible, redacted int
	if err := s.DB.QueryRow(ctx, `SELECT count(*) FILTER (WHERE NOT redacted), count(*) FILTER (WHERE redacted AND text IS NULL AND author_pk IS NULL)
		FROM comments WHERE author_pk = $1 OR (redacted AND author_key IN (SELECT author_key FROM comments WHERE redacted))`, authorPK).Scan(&visible, &redacted); err != nil {
		t.Fatal(err)
	}
	if visible != 0 || redacted == 0 {
		t.Fatalf("after the author erasure: %d visible, %d redacted", visible, redacted)
	}
	var authors int
	_ = s.DB.QueryRow(ctx, `SELECT count(*) FROM comment_authors WHERE author_pk = $1`, authorPK).Scan(&authors)
	if authors != 0 {
		t.Fatalf("the author's row survived")
	}
	// Seen again: stored redacted.
	s.Wait(t, 3*time.Minute, s.Crawl(t, p.Username, full, lim, nil))
	if err := s.DB.QueryRow(ctx, `SELECT count(*) FROM comments WHERE author_pk = $1`, authorPK).Scan(&visible); err != nil || visible != 0 {
		t.Fatalf("the erased author's comments were written back: %d %v", visible, err)
	}

	// The whole profile.
	if code := s.Do(t, admin, "POST", "/v1/erasure-requests", map[string]any{"subject_type": "profile", "username": p.Username}, &e); code != http.StatusAccepted {
		t.Fatalf("profile erasure: %d", code)
	}
	waitErasure(t, s, admin, e.ID.String())
	count := func(q string) int {
		var n int
		if err := s.DB.QueryRow(ctx, q, p.PK).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if n := count(`SELECT count(*) FROM posts WHERE owner_pk = $1`) + count(`SELECT count(*) FROM comments WHERE owner_pk = $1`) +
		count(`SELECT count(*) FROM highlights WHERE user_pk = $1`) + count(`SELECT count(*) FROM profile_snapshots WHERE user_pk = $1`); n != 0 {
		t.Fatalf("%d rows of the erased profile left", n)
	}
	if count(`SELECT count(*) FROM profiles WHERE user_pk = $1 AND status = 'suppressed' AND username IS NULL`) != 1 {
		t.Fatal("the profile row is not suppressed")
	}
	if count(`SELECT count(*) FROM posts WHERE owner_pk = $1`) != 0 {
		t.Fatal("posts left")
	}
	// A crawl after the erasure writes nothing of it; the other profile is
	// untouched.
	s.Wait(t, 3*time.Minute, s.Crawl(t, p.Username, full, lim, &igstaskv1.ProfileScope{UserPk: p.PK}))
	if n := count(`SELECT count(*) FROM posts WHERE owner_pk = $1`); n != 0 {
		t.Fatalf("%d posts written back after the erasure", n)
	}
	var others int
	_ = s.DB.QueryRow(ctx, `SELECT count(*) FROM posts WHERE owner_pk = $1`, other.PK).Scan(&others)
	if others == 0 {
		t.Fatal("the other profile's posts went too")
	}
}

func waitErasure(t *testing.T, s *Stack, key, id string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		var e api.Erasure
		if code := s.Do(t, key, "GET", "/v1/erasure-requests/"+id, nil, &e); code != http.StatusOK {
			t.Fatalf("get erasure: %d", code)
		}
		if e.Status == "done" {
			return
		}
		if e.Status == "failed" || time.Now().After(deadline) {
			t.Fatalf("erasure %s: %s %v", id, e.Status, e.Error)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
