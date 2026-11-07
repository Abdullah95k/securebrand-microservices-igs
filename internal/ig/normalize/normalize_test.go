package normalize

import (
	"encoding/json"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/common/v1"
	contentv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/content/v1"
	igv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/ig/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/topics"
)

var at = time.Date(2026, 11, 4, 9, 0, 0, 0, time.UTC)

func obs(id string) *commonv1.Observation {
	return &commonv1.Observation{EventId: id, ObservedAt: timestamppb.New(at), Platform: commonv1.Platform_PLATFORM_INSTAGRAM,
		Version: at.UnixMicro(), Collector: "igs", CollectorVersion: "v1.2.3", RawRef: "s3://raw/k#1"}
}

func i64(n int64) *int64 { return &n }

func one[T proto.Message](t *testing.T, outs []Out, topic string) T {
	t.Helper()
	var got []T
	for _, o := range outs {
		if o.Topic == topic {
			got = append(got, o.Msg.(T))
		}
	}
	if len(got) != 1 {
		t.Fatalf("%d events on %s, want 1: %v", len(got), topic, outs)
	}
	return got[0]
}

func checkEnvelope(t *testing.T, e *contentv1.Envelope, native string) {
	t.Helper()
	if e.Platform != "instagram" || e.NativeId != native || e.Version != at.UnixMicro() || e.Collector != "igs" ||
		e.CollectorVersion != "v1.2.3" || e.RawRef != "s3://raw/k#1" || e.EventId == "" || !e.ObservedAt.AsTime().Equal(at) {
		t.Fatalf("envelope %+v", e)
	}
}

// A profile becomes an account with everything the entity layer links on
// (bio, website, profile links, the linked Facebook page) and an account
// metrics sample.
func TestProfileToAccount(t *testing.T) {
	outs := Map(&igv1.Profile{Obs: obs("e1"), UserPk: 42, Username: "brand", FullName: "Brand", Biography: "Baghdad",
		BioLinks:           []*igv1.BioLink{{Url: "https://brand.iq", Title: "site"}, {Url: "https://youtube.com/@brand"}},
		LinkedFacebookPage: "Brand Page", LinkedFacebookPageId: "540790833730240", IsVerified: true, Category: "Shop", IsBusiness: true,
		FollowerCount: i64(1000), FollowingCount: i64(10), MediaCount: i64(55), ProfilePic: &commonv1.ImageRef{Url: "https://cdn/p.jpg"},
		Status: igv1.ProfileStatus_PROFILE_STATUS_ACTIVE})
	a := one[*contentv1.Account](t, outs, topics.ContentAccount)
	checkEnvelope(t, a.Envelope, "42")
	if a.Handle != "brand" || a.Name != "Brand" || a.Bio != "Baghdad" || a.Website != "https://brand.iq" || len(a.Links) != 3 ||
		a.Links[2].Url != "https://www.facebook.com/540790833730240" || a.Links[2].Title != "Brand Page" || a.Links[2].Platform != "fb" ||
		a.Links[2].PlatformId != "540790833730240" || !a.Verified || !a.IsBusiness || a.IsPrivate || a.IsDeleted ||
		a.GetFollowers() != 1000 || a.PictureUrl != "https://cdn/p.jpg" || a.Kind != "profile" {
		t.Fatalf("account %+v", a)
	}
	m := one[*contentv1.Metrics](t, outs, topics.ContentMetrics)
	if m.Target != "account" || m.GetFollowers() != 1000 || m.GetPostCount() != 55 {
		t.Fatalf("account metrics %+v", m)
	}
	gone := one[*contentv1.Account](t, Map(&igv1.Profile{Obs: obs("e2"), UserPk: 43, Status: igv1.ProfileStatus_PROFILE_STATUS_UNAVAILABLE}),
		topics.ContentAccount)
	if !gone.IsDeleted {
		t.Fatal("an unavailable profile is not deleted")
	}
	private := one[*contentv1.Account](t, Map(&igv1.Profile{Obs: obs("e3"), UserPk: 44, Status: igv1.ProfileStatus_PROFILE_STATUS_PRIVATE}),
		topics.ContentAccount)
	if !private.IsPrivate {
		t.Fatal("a private profile is not private")
	}
}

// A post keeps its kind, counts (likes left out when hidden), co-authors,
// tags and Instagram's own fields in extras.
func TestPostAndMetrics(t *testing.T) {
	outs := Map(&igv1.Post{Obs: obs("p1"), MediaPk: 7, Shortcode: "Cabc", OwnerPk: 42, TakenAt: timestamppb.New(at.Add(-time.Hour)),
		MediaType: igv1.MediaType_MEDIA_TYPE_VIDEO, ProductType: "clips", Caption: "hello #iraq", Hashtags: []string{"iraq"},
		LikeCount: i64(3), CountsHidden: true, CommentCount: i64(9), PlayCount: i64(1000),
		Coauthors: []*igv1.Coauthor{{UserPk: 42}, {UserPk: 77}}, Usertags: []*igv1.UserTag{{UserPk: 88}},
		Location: &igv1.Location{Pk: 5, Name: "Baghdad"}, Status: igv1.PostStatus_POST_STATUS_VISIBLE})
	p := one[*contentv1.Post](t, outs, topics.ContentPost)
	checkEnvelope(t, p.Envelope, "7")
	if p.Kind != "reel" || p.AccountId != "42" || p.Text != "hello #iraq" || p.Likes != nil || p.GetViews() != 1000 || p.GetComments() != 9 ||
		len(p.CoauthorIds) != 1 || p.CoauthorIds[0] != "77" || p.Url != "https://www.instagram.com/p/Cabc/" ||
		!p.Envelope.PublishedAt.AsTime().Equal(at.Add(-time.Hour)) {
		t.Fatalf("post %+v", p)
	}
	var x map[string]any
	if err := json.Unmarshal([]byte(p.ExtrasJson), &x); err != nil || x["shortcode"] != "Cabc" || x["counts_hidden"] != true ||
		x["location"] == nil || x["tagged_ids"] == nil {
		t.Fatalf("extras %s %v", p.ExtrasJson, err)
	}
	m := one[*contentv1.Metrics](t, Map(&igv1.PostMetrics{Obs: obs("m1"), MediaPk: 7, OwnerPk: 42, CapturedAt: timestamppb.New(at),
		LikeCount: i64(5), CommentCount: i64(9)}), topics.ContentMetrics)
	if m.Target != "post" || m.ParentId != "42" || m.GetLikes() != 5 || m.GetComments() != 9 {
		t.Fatalf("post metrics %+v", m)
	}
	gone := one[*contentv1.Post](t, Map(&igv1.Post{Obs: obs("p2"), MediaPk: 8, Status: igv1.PostStatus_POST_STATUS_UNAVAILABLE}), topics.ContentPost)
	if !gone.IsDeleted {
		t.Fatal("an unavailable post is not deleted")
	}
}

// A comment always has its post; a reply its parent; its likes and
// replies also go out as a comment metrics sample.
func TestComment(t *testing.T) {
	outs := Map(&igv1.Comment{Obs: obs("c1"), CommentPk: 9, MediaPk: 7, OwnerPk: 42, ParentCommentPk: i64(8), Text: "nice",
		CreatedAt: timestamppb.New(at), LikeCount: i64(2), ChildCommentCount: i64(1),
		Author: &igv1.Author{UserPk: 600, Username: "fan", AuthorKey: []byte("k")}, Status: igv1.CommentStatus_COMMENT_STATUS_VISIBLE})
	c := one[*contentv1.Comment](t, outs, topics.ContentComment)
	checkEnvelope(t, c.Envelope, "9")
	if c.PostId != "7" || c.ParentCommentId != "8" || c.AccountId != "42" || c.Text != "nice" || c.Author.AuthorId != "600" ||
		c.Author.Name != "fan" || string(c.Author.AuthorKey) != "k" || c.GetLikes() != 2 || c.GetReplies() != 1 {
		t.Fatalf("comment %+v", c)
	}
	m := one[*contentv1.Metrics](t, outs, topics.ContentMetrics)
	if m.Target != "comment" || m.ParentId != "7" || m.GetLikes() != 2 {
		t.Fatalf("comment metrics %+v", m)
	}
	// Keyed by its post, so a post's comments stay in order.
	for _, o := range outs {
		if o.Topic == topics.ContentComment && o.Key != "instagram:7" {
			t.Fatalf("comment key %q", o.Key)
		}
	}
}

// A deleted comment's event (REQ-008 fix 14) carries no text: it maps to a
// content.comment marked deleted, dated by the comment's creation (so a
// store partitioned by it puts the deletion beside the comment), and no
// metrics sample.
func TestCommentDeleted(t *testing.T) {
	outs := Map(&igv1.Comment{Obs: obs("c2"), CommentPk: 9, MediaPk: 7, OwnerPk: 42, CreatedAt: timestamppb.New(at),
		Status: igv1.CommentStatus_COMMENT_STATUS_DELETED})
	c := one[*contentv1.Comment](t, outs, topics.ContentComment)
	if !c.IsDeleted || c.Text != "" || !c.Envelope.GetPublishedAt().AsTime().Equal(at) {
		t.Fatalf("deleted comment %+v", c)
	}
	for _, o := range outs {
		if o.Topic == topics.ContentMetrics {
			t.Fatalf("a deletion published metrics: %+v", o.Msg)
		}
	}
}

// A walled post (REQ-008 fix 15) is published from the grid's fields as
// unavailable, not deleted, with no counts; a gone one is deleted.
func TestWalledPost(t *testing.T) {
	outs := Map(&igv1.Post{Obs: obs("w1"), MediaPk: 11, Shortcode: "WAL1", OwnerPk: 42, TakenAt: timestamppb.New(at),
		MediaType: igv1.MediaType_MEDIA_TYPE_VIDEO, ProductType: "clips", Status: igv1.PostStatus_POST_STATUS_UNAVAILABLE,
		StatusReason: "login_wall"})
	p := one[*contentv1.Post](t, outs, topics.ContentPost)
	var x map[string]any
	if err := json.Unmarshal([]byte(p.ExtrasJson), &x); err != nil {
		t.Fatal(err)
	}
	if p.IsDeleted || p.Likes != nil || p.Comments != nil || p.Views != nil || p.Kind != "reel" ||
		p.Url != "https://www.instagram.com/p/WAL1/" || x["status"] != "unavailable" || x["status_reason"] != "login_wall" {
		t.Fatalf("walled post %+v, extras %v", p, x)
	}
	for _, o := range outs {
		if o.Topic == topics.ContentMetrics {
			t.Fatalf("a walled post published metrics: %+v", o.Msg)
		}
	}
	gone := one[*contentv1.Post](t, Map(&igv1.Post{Obs: obs("w2"), MediaPk: 11, OwnerPk: 42, TakenAt: timestamppb.New(at),
		Status: igv1.PostStatus_POST_STATUS_UNAVAILABLE, StatusReason: "not_found"}), topics.ContentPost)
	if !gone.IsDeleted {
		t.Fatalf("gone post %+v, want deleted", gone)
	}
}

// Highlight items are posts of kind highlight.
func TestHighlightItemsArePosts(t *testing.T) {
	outs := Map(&igv1.Highlight{Obs: obs("h1"), HighlightId: 17, UserPk: 42, Title: "Trips",
		Items: []*igv1.HighlightItem{{ItemPk: 3_000_000_000_000_000_001, MediaType: 2, MediaIds: []string{"a"}},
			{ItemPk: 3_000_000_000_000_000_002, TakenAt: timestamppb.New(at)}}})
	if len(outs) != 2 {
		t.Fatalf("%d events, want 2", len(outs))
	}
	for _, o := range outs {
		p := o.Msg.(*contentv1.Post)
		if o.Topic != topics.ContentPost || p.Kind != "highlight" || p.AccountId != "42" || p.Envelope.PublishedAt == nil ||
			p.Url != "https://www.instagram.com/stories/highlights/17/" {
			t.Fatalf("highlight item %+v", p)
		}
	}
	if outs[0].Msg.(*contentv1.Post).Envelope.EventId == outs[1].Msg.(*contentv1.Post).Envelope.EventId {
		t.Fatal("two items share an event id")
	}
}

// Media goes to its owner: a carousel child to its post, a comment's to the
// comment, a profile picture to the account.
func TestMediaOwners(t *testing.T) {
	cases := []struct {
		m         *igv1.Media
		ownerType string
		owner     string
		typ       string
	}{
		{&igv1.Media{Obs: obs("x1"), MediaId: "m1", OwnerType: "post", MediaPk: 7, Kind: commonv1.MediaKind_MEDIA_KIND_PHOTO}, "post", "7", "image"},
		{&igv1.Media{Obs: obs("x2"), MediaId: "m2", OwnerType: "post", MediaPk: 71, ParentMediaPk: i64(7), Kind: commonv1.MediaKind_MEDIA_KIND_VIDEO}, "post", "7", "video"},
		{&igv1.Media{Obs: obs("x3"), MediaId: "m3", OwnerType: "comment", CommentPk: i64(9)}, "comment", "9", "image"},
		{&igv1.Media{Obs: obs("x4"), MediaId: "m4", OwnerType: "profile", UserPk: 42}, "account", "42", "image"},
	}
	for _, c := range cases {
		m := one[*contentv1.Media](t, Map(c.m), topics.ContentMedia)
		if m.OwnerType != c.ownerType || m.OwnerId != c.owner || m.Type != c.typ {
			t.Errorf("%s: %+v", c.m.MediaId, m)
		}
	}
}

// Record decodes by topic; unknown topics give nothing; garbage is an error.
func TestRecord(t *testing.T) {
	b, _ := proto.Marshal(&igv1.Post{Obs: obs("p"), MediaPk: 1})
	if outs, err := Record(topics.Post, b); err != nil || len(outs) != 1 {
		t.Fatalf("%v %v", outs, err)
	}
	if outs, err := Record(topics.Job, b); err != nil || outs != nil {
		t.Fatalf("job topic mapped: %v %v", outs, err)
	}
	if _, err := Record(topics.Post, []byte{0xff, 0xff}); err == nil {
		t.Fatal("garbage decoded")
	}
}
