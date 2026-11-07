package igdisc

import (
	"slices"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	igv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/ig/v1"
)

// Every example is synthetic.

var now = time.Date(2026, 11, 7, 9, 0, 0, 0, time.UTC)

func newTestRecorder(t *testing.T, cfg Config) *Recorder {
	t.Helper()
	r, err := NewRecorder(nil, cfg)
	if err != nil {
		t.Fatal(err)
	}
	r.Now = func() time.Time { return now }
	return r
}

func ids(ks []Keyword) []string {
	var out []string
	for _, k := range ks {
		out = append(out, k.ID)
	}
	return out
}

// The seed's keywords match as igs's discovery matches them, and each
// match names the keyword by its contract ID, computed from the list's
// spelling.
func TestIndexMatchesTheSharedList(t *testing.T) {
	x, err := SeedIndex()
	if err != nil {
		t.Fatal(err)
	}
	if x.Len() < 300 {
		t.Fatalf("seed index holds %d keywords", x.Len())
	}
	for _, tc := range []struct {
		text string
		want []string
	}{
		// A prefix attached to an Arabic name, and a nisba ending.
		{"صباح الخير من وبغداد", []string{"baghdad:ar:بغداد"}},
		{"اكلة بغدادية", []string{"baghdad:ar:بغداد"}},
		// A word that only holds a name is not a match (عراقة, "heritage").
		{"عراقة المكان", nil},
		// Sorani typed with ه and a ZWNJ is the same keyword as with ە.
		{"ئێوارەیەک لە هه\u200cولێر", []string{"erbil:ckb:ههولێر"}},
		// One spelling listed in two languages matches as both.
		{"Back in Iraq today", []string{"iraq:en:iraq", "iraq:kmr:iraq"}},
		// Keywords loaded off (everyday words) never match.
		{"a hit song", nil},
		{"", nil},
	} {
		got := ids(x.Match(tc.text))
		if !slices.Equal(got, tc.want) {
			t.Errorf("%q: %v, want %v", tc.text, got, tc.want)
		}
	}
}

func ts(t time.Time) *timestamppb.Timestamp { return timestamppb.New(t) }

func post(caption, place string, age time.Duration) *igv1.Post {
	p := &igv1.Post{MediaPk: 3000000000000000001, Shortcode: "FakeCode01", OwnerPk: 501, TakenAt: ts(now.Add(-age)), Caption: caption,
		Usertags:  []*igv1.UserTag{{UserPk: 601, Username: "Tagged_One"}, {UserPk: 501, Username: "owner_itself"}},
		Coauthors: []*igv1.Coauthor{{UserPk: 602, Username: "co_author"}},
		Mentions:  []string{"@mentioned.one", "not a username!"}}
	if place != "" {
		p.Location = &igv1.Location{Pk: 200000001, Name: place}
	}
	return p
}

func pending(r *Recorder) map[evidenceKey]Evidence { return r.pending }

// A recent post whose caption names a keyword gives evidence for each
// account tagged, co-authoring or mentioned in it, never its owner; its
// place gives evidence of its own.
func TestPostEvidence(t *testing.T) {
	r := newTestRecorder(t, Config{Window: 6 * time.Hour})
	r.Post(post("جلسة تصوير في البصرة", "Erbil Citadel", 2*time.Hour))
	got := pending(r)
	want := map[evidenceKey]string{
		{"tagged_one", "basra:ar:البصره", "ig:post:3000000000000000001"}:    ViaCaption,
		{"co_author", "basra:ar:البصره", "ig:post:3000000000000000001"}:     ViaCaption,
		{"mentioned.one", "basra:ar:البصره", "ig:post:3000000000000000001"}: ViaCaption,
		// The list holds Basra with and without the article: both match.
		{"tagged_one", "basra:ar:بصره", "ig:post:3000000000000000001"}:     ViaCaption,
		{"co_author", "basra:ar:بصره", "ig:post:3000000000000000001"}:      ViaCaption,
		{"mentioned.one", "basra:ar:بصره", "ig:post:3000000000000000001"}:  ViaCaption,
		{"tagged_one", "erbil:en:erbil", "ig:post:3000000000000000001"}:    ViaPostPlace,
		{"co_author", "erbil:en:erbil", "ig:post:3000000000000000001"}:     ViaPostPlace,
		{"mentioned.one", "erbil:en:erbil", "ig:post:3000000000000000001"}: ViaPostPlace,
	}
	if len(got) != len(want) {
		t.Fatalf("got %d pieces of evidence, want %d: %v", len(got), len(want), got)
	}
	for k, via := range want {
		e, ok := got[k]
		if !ok {
			t.Errorf("missing %v", k)
			continue
		}
		if e.Via != via || e.SourcePK != 501 || e.URL != "https://www.instagram.com/p/FakeCode01/" || !e.ContentTime.Equal(now.Add(-2*time.Hour)) {
			t.Errorf("%v: %+v", k, e)
		}
	}
	if e := got[evidenceKey{"tagged_one", "basra:ar:البصره", "ig:post:3000000000000000001"}]; e.UserPK != 601 || e.Keyword.Term != "البصرة" ||
		e.Keyword.Language != "ar" || e.Keyword.Group != "basra" {
		t.Errorf("tagged account's evidence: %+v", e)
	}
}

// Content older than the window, a caption naming nothing, and a post no
// one else is on give nothing.
func TestPostWithoutEvidence(t *testing.T) {
	r := newTestRecorder(t, Config{Window: 6 * time.Hour})
	r.Post(post("جلسة تصوير في البصرة", "", 7*time.Hour))
	r.Post(post("latte art", "London", time.Hour))
	p := post("Baghdad nights", "", time.Hour)
	p.Usertags, p.Coauthors, p.Mentions = nil, nil, nil
	r.Post(p)
	r.Post(nil)
	var nilRec *Recorder
	nilRec.Post(post("Baghdad", "", time.Hour))
	nilRec.Comments(1, nil)
	if n := len(pending(r)); n != 0 {
		t.Fatalf("%d pieces of evidence: %v", n, pending(r))
	}
}

func comment(pk int64, user string, upk int64, text string, age time.Duration) *igv1.Comment {
	return &igv1.Comment{CommentPk: pk, MediaPk: 3000000000000000001, OwnerPk: 501, Text: text, CreatedAt: ts(now.Add(-age)),
		Author: &igv1.Author{UserPk: upk, Username: user}}
}

// Recent comments that name a keyword give evidence for their authors,
// only when commenters count (privacy.pii_mode full); the owner's own
// comments, redacted ones, erased authors and old comments do not.
func TestCommentEvidence(t *testing.T) {
	cs := []*igv1.Comment{
		comment(1, "commenter_0001", 701, "تحياتي من اربيل", time.Hour),
		comment(2, "commenter_0002", 702, "nice photo", time.Hour),
		comment(3, "commenter_0003", 703, "من الموصل", 8*time.Hour),
		comment(4, "commenter_0004", 704, "Mosul forever", time.Hour),
	}
	op := comment(5, "owner_itself", 501, "شكرا من بغداد", time.Hour)
	op.IsOp = true
	red := comment(6, "", 0, "", time.Hour)
	red.Redacted = true
	cs = append(cs, op, red)

	off := newTestRecorder(t, Config{Window: 6 * time.Hour})
	off.Comments(501, cs)
	if n := len(pending(off)); n != 0 {
		t.Fatalf("commenters off: %d pieces of evidence", n)
	}

	r := newTestRecorder(t, Config{Window: 6 * time.Hour, Commenters: true})
	r.Suppressed = func(pk int64) bool { return pk == 704 }
	r.Comments(501, cs)
	got := pending(r)
	if len(got) != 1 {
		t.Fatalf("got %v, want commenter_0001's evidence alone", got)
	}
	e, ok := got[evidenceKey{"commenter_0001", "erbil:ar:اربیل", "ig:comment:1"}]
	if !ok {
		t.Fatalf("got %v", got)
	}
	if e.Via != ViaComment || e.UserPK != 701 || e.SourcePK != 501 || e.URL != "" {
		t.Errorf("%+v", e)
	}
}

// The same account, keyword and content are held once; a later sighting
// with the user pk fills it in.
func TestEvidenceHeldOnce(t *testing.T) {
	r := newTestRecorder(t, Config{Window: 6 * time.Hour})
	p := post("Baghdad", "", time.Hour)
	p.Usertags, p.Coauthors = nil, nil
	p.Mentions = []string{"same_one", "Same_One"}
	r.Post(p)
	p2 := post("Baghdad", "", time.Hour)
	p2.Usertags, p2.Coauthors, p2.Mentions = []*igv1.UserTag{{UserPk: 777, Username: "same_one"}}, nil, nil
	r.Post(p2)
	got := pending(r)
	if len(got) != 1 {
		t.Fatalf("got %v", got)
	}
	for _, e := range got {
		if e.UserPK != 777 {
			t.Errorf("user pk %d, want 777", e.UserPK)
		}
	}
}

// The window and defaults: 6 hours, 10 seconds, priority 2.
func TestDefaults(t *testing.T) {
	r := newTestRecorder(t, Config{})
	if r.Cfg.Window != 6*time.Hour || r.Cfg.FlushEvery != 10*time.Second || r.Cfg.Priority != 2 {
		t.Fatalf("%+v", r.Cfg)
	}
	if !r.recent(now.Add(-6*time.Hour)) || r.recent(now.Add(-6*time.Hour-time.Second)) || r.recent(time.Time{}) {
		t.Error("window edges")
	}
}
