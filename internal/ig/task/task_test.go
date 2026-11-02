package task

import (
	"testing"
	"time"

	commonv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/common/v1"
	igstaskv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/igs/task/v1"
)

func TestNormalizeRef(t *testing.T) {
	for in, want := range map[string]string{
		"Some.User": "u:some.user", "@some_user": "u:some_user", "https://www.instagram.com/some.user/": "u:some.user",
		"instagram.com/some.user?hl=en": "u:some.user", "id:12345": "id:12345",
	} {
		if got, err := NormalizeRef(in); err != nil || got != want {
			t.Errorf("%q: %q %v, want %q", in, got, err, want)
		}
	}
	for _, bad := range []string{"", "https://www.instagram.com/p/ABC/", "not a name", "id:x", "https://www.instagram.com/reels/"} {
		if got, err := NormalizeRef(bad); err == nil {
			t.Errorf("%q accepted as %q", bad, got)
		}
	}
}

func TestIDsAndContinuations(t *testing.T) {
	a := NewProfile("j", igstaskv1.Lane_LANE_P1, commonv1.CrawlMode_CRAWL_MODE_FULL, "u:x", 0, nil, nil)
	b := NewProfile("j", igstaskv1.Lane_LANE_P1, commonv1.CrawlMode_CRAWL_MODE_FULL, "u:x", 0, nil, nil)
	if a.TaskId != b.TaskId || a.GetProfile().Username != "x" || ScopeKey(a) != "profileref:u:x" {
		t.Fatalf("deterministic ids: %s %s %s", a.TaskId, b.TaskId, ScopeKey(a))
	}
	a.ItemsSeen = 7
	c := Continuation(a, "cur")
	if c.TaskId == a.TaskId || c.LineageId != a.LineageId || c.Seq != 1 || c.Cursor != "cur" || c.ItemsSeen != 7 {
		t.Fatalf("continuation: %+v", c)
	}
	if r := Retry(a, ""); r.TaskId != a.TaskId || r.Attempt != 1 {
		t.Fatalf("retry: %+v", r)
	}
	if rp := Replay(a, time.Unix(5, 0)); rp.TaskId == a.TaskId || rp.LineageId != a.LineageId {
		t.Fatalf("replay: %+v", rp)
	}
	p := NewPost(a, commonv1.CrawlMode_CRAWL_MODE_FULL, &igstaskv1.PostScope{MediaPk: 9})
	if p.JobId != "j" || ScopeKey(p) != "post:9" || p.Kind != igstaskv1.Kind_KIND_POST {
		t.Fatalf("post: %+v", p)
	}
}
