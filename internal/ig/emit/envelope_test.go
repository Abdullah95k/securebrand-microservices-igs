package emit

import (
	"testing"
	"time"

	commonv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/common/v1"
	igv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/ig/v1"
)

// Every event carries the shared contract's envelope (architecture §5):
// platform, native id, a version that grows with each observation, the
// collector and its build, and the raw response's reference; the content
// hash and event id ignore the envelope.
func TestEnvelope(t *testing.T) {
	at := time.Date(2026, 11, 4, 9, 0, 0, 0, time.UTC)
	e := &Emitter{Now: func() time.Time { return at }}
	p := &igv1.Post{MediaPk: 3_000_000_000_000_000_001, Caption: "x"}
	src := Source{JobID: "j1", Query: "post_page", RawRef: "s3://raw/raw/post_page/2026/11/04/09/pod-1-000001.jsonl.zst#3"}
	e.stamp(src, "post:3000000000000000001", p, func(o *commonv1.Observation) { p.Obs = o })
	o := p.Obs
	if o.Platform != commonv1.Platform_PLATFORM_INSTAGRAM || o.NativeId != "3000000000000000001" || o.Version != at.UnixMicro() ||
		o.Collector != "igs" || o.CollectorVersion == "" || o.RawRef != src.RawRef {
		t.Fatalf("envelope %+v", o)
	}
	first := o.EventId
	later := at.Add(time.Hour)
	e.Now = func() time.Time { return later }
	src.RawRef = "s3://raw/other#1"
	e.stamp(src, "post:3000000000000000001", p, func(o *commonv1.Observation) { p.Obs = o })
	if p.Obs.Version <= o.Version {
		t.Fatalf("version did not grow: %d then %d", o.Version, p.Obs.Version)
	}
	if p.Obs.EventId != first {
		t.Fatal("the same content got another event id because its envelope changed")
	}
}
