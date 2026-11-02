// Package fakeig is a synthetic logged-out Instagram for tests: profile and
// post pages carrying tokens and cookies, and the five GraphQL operations of
// the owner's recordings (docs/instagram/har-findings-2026-10-30.md), with
// faults that model the open questions (U1 grid depth, U3 client-state
// fields, U5 identity-bound cursors) and the walls. Response shapes follow
// the findings' field lists; everything else about them is a stand-in. All
// data is generated: no real account, name or text.
package fakeig

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"time"
)

// Spec sizes a world.
type Spec struct {
	Seed            uint64
	Profiles        int
	PostsPerProfile int
	MaxComments     int // per post, uniform in [0, MaxComments]
	Highlights      int // per profile
	PostSpacing     time.Duration
	// Pinned older posts per profile, shown first in the grid as on
	// Instagram (up to 3).
	Pinned int
	// GatedEvery gates every Nth post (from the 2nd): its page draws a
	// login wall in place of the post, as for age-restricted posts.
	GatedEvery int
	// HiddenLikesEvery hides the like and view counts of every Nth post
	// (from the 3rd); Instagram then serves a like_count of 3.
	HiddenLikesEvery int
	// RepliesEvery gives every Nth comment replies, which logged-out
	// readers see only as a count (and in the post's comment count).
	RepliesEvery int
}

// DefaultSpec is a small world: 3 profiles of 40 posts.
func DefaultSpec() Spec {
	return Spec{Seed: 1, Profiles: 3, PostsPerProfile: 40, MaxComments: 25, Highlights: 2, PostSpacing: 18 * time.Hour}
}

// Profile is one account.
type Profile struct {
	PK int64
	// ID is the user object's "id", which the grid's _connection query
	// takes; it differs from the pk, as on Instagram.
	ID         string
	Username   string
	FullName   string
	Bio        string
	Followers  int64
	Following  int64
	Verified   bool
	Posts      []*Post // newest first
	Highlights []Highlight
	// Pinned posts come first in the grid, before the rest newest first.
	Pinned []*Post
}

// Reels is the profile's videos, newest first: its reels tab.
func (p *Profile) Reels() []*Post {
	var out []*Post
	for _, x := range p.Posts {
		if x.MediaType == 2 && !x.Deleted {
			out = append(out, x)
		}
	}
	return out
}

// Grid is the profile's posts in grid order: pinned first, then the rest
// newest first.
func (p *Profile) Grid() []*Post {
	var out []*Post
	for _, x := range p.Pinned {
		if !x.Deleted {
			out = append(out, x)
		}
	}
	for _, x := range p.Posts {
		if !x.Deleted && !slices.Contains(p.Pinned, x) {
			out = append(out, x)
		}
	}
	return out
}

// Post is one grid item.
type Post struct {
	PK          int64
	Code        string
	MediaType   int // 1 photo, 2 video, 8 carousel
	ProductType string
	Caption     string
	TakenAt     time.Time
	Likes       int64
	Comments    []Comment // newest first
	Owner       *Profile
	// What only the post page shows (derived from the post's place, so
	// the generator's random sequence is unchanged).
	Tagged        []string // usernames
	Coauthors     []string // usernames
	Location      string
	Topics        []string
	DurationS     float64 // videos
	Edited        bool
	AILabel       string
	CarouselCount int
	// Plays is a video's view count, shown logged out only in the
	// profile's reels tab.
	Plays int64
	// Gated posts show a login wall in place of their detail.
	Gated bool
	// LikesHidden: the owner hid the like and view counts.
	LikesHidden bool
	// Deleted posts leave the grid; their page answers 404.
	Deleted bool
}

// CommentCount is the post's comment count as Instagram reports it:
// top-level comments and their replies.
func (p *Post) CommentCount() int64 {
	n := int64(len(p.Comments))
	for _, c := range p.Comments {
		n += int64(c.Replies)
	}
	return n
}

// Comment is a top-level comment.
type Comment struct {
	PK        int64
	Text      string
	CreatedAt time.Time
	Likes     int64
	UserPK    int64
	Username  string
	// Replies is the comment's reply count; logged out, the replies
	// themselves are not served.
	Replies int
}

// Highlight is one highlights reel.
type Highlight struct {
	ID    int64
	Title string
	Items int
}

// World is the generated ground truth. Its methods are safe while the
// server serves it.
type World struct {
	mu       sync.RWMutex
	r        *rand.Rand
	spec     Spec
	nextPK   int64
	profiles []*Profile
	byName   map[string]*Profile
	byPK     map[int64]*Profile
	byID     map[string]*Profile
	posts    map[int64]*Post
}

// Epoch is the newest post's time in every world.
var Epoch = time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)

// NewWorld generates a world from s.
func NewWorld(s Spec) *World {
	r := rand.New(rand.NewPCG(s.Seed, 0x1a))
	w := &World{r: r, spec: s, nextPK: 3_000_000_000_000_000_000,
		byName: map[string]*Profile{}, byPK: map[int64]*Profile{}, byID: map[string]*Profile{}, posts: map[int64]*Post{}}
	for i := range s.Profiles {
		p := &Profile{PK: 50_000_000_000 + int64(i)*7919, Username: fmt.Sprintf("profile_%02d", i+1),
			FullName: fmt.Sprintf("Profile %02d", i+1), Bio: "synthetic biography",
			Followers: 1000 + r.Int64N(100000), Following: r.Int64N(500), Verified: i%2 == 0}
		p.ID = fmt.Sprintf("178414%011d", p.PK%100_000_000_000)
		for j := range s.PostsPerProfile {
			post := w.makePost(p, i, j, Epoch.Add(-time.Duration(j)*s.PostSpacing))
			p.Posts = append(p.Posts, post)
			w.posts[post.PK] = post
		}
		for k := range min(s.Pinned, 3) {
			if at := 4 * (k + 1); at < len(p.Posts) {
				p.Pinned = append(p.Pinned, p.Posts[at])
			}
		}
		for h := range s.Highlights {
			p.Highlights = append(p.Highlights, Highlight{ID: 17_900_000_000_000_000 + int64(i*100+h), Title: fmt.Sprintf("Highlight %d", h+1), Items: 1 + r.IntN(5)})
		}
		w.profiles = append(w.profiles, p)
		w.byName[p.Username] = p
		w.byPK[p.PK] = p
		w.byID[p.ID] = p
	}
	return w
}

func (w *World) pk() int64 { w.nextPK += 1 + w.r.Int64N(1000); return w.nextPK }

// makePost generates post j of profile i (j picks the derived fields).
func (w *World) makePost(p *Profile, i, j int, taken time.Time) *Post {
	r, s := w.r, w.spec
	post := &Post{PK: MediaPK(taken, r.Int64N(1<<23)), MediaType: []int{1, 2, 8}[r.IntN(3)], Caption: fmt.Sprintf("synthetic caption %d", j+1),
		TakenAt: taken, Likes: r.Int64N(5000), Owner: p}
	post.Code = shortcode(post.PK)
	post.ProductType = map[int]string{1: "feed", 2: "clips", 8: "carousel_container"}[post.MediaType]
	if j%3 == 0 {
		post.Tagged = []string{fmt.Sprintf("tagged_%02d_%d", i+1, j)}
	}
	if j%5 == 0 {
		post.Coauthors = []string{fmt.Sprintf("collab_%02d", i+1)}
	}
	if j%4 == 0 {
		post.Location = fmt.Sprintf("City %d", j%7)
	}
	post.Topics = []string{fmt.Sprintf("topic %d", j%6), "synthetic"}
	if post.MediaType == 2 {
		post.DurationS = float64(10+j%50) + 0.25
		post.Plays = post.Likes*12 + int64(j)*37 + 100
	}
	if post.MediaType == 8 {
		post.CarouselCount = 2 + j%4
	}
	post.Edited = j%7 == 0
	post.Gated = s.GatedEvery > 0 && j%s.GatedEvery == 1
	post.LikesHidden = s.HiddenLikesEvery > 0 && j%s.HiddenLikesEvery == 2
	if j%9 == 0 {
		post.AILabel = "AI content"
	}
	n := 0
	if s.MaxComments > 0 {
		n = r.IntN(s.MaxComments + 1)
	}
	for k := range n {
		post.Comments = append(post.Comments, w.makeComment(post.TakenAt.Add(time.Duration(n-k)*time.Minute), n-k, k))
	}
	return post
}

func (w *World) makeComment(at time.Time, nth, k int) Comment {
	r := w.r
	cm := Comment{PK: w.pk(), Text: fmt.Sprintf("synthetic comment %d", nth), CreatedAt: at, Likes: r.Int64N(50),
		UserPK: 60_000_000_000 + r.Int64N(1_000_000), Username: fmt.Sprintf("commenter_%04d", r.IntN(10000))}
	if w.spec.RepliesEvery > 0 && k%w.spec.RepliesEvery == 0 {
		cm.Replies = 1 + k%3
	}
	return cm
}

// Profiles returns every profile.
func (w *World) Profiles() []*Profile { return w.profiles }

// Profile returns the profile with username, or nil.
func (w *World) Profile(username string) *Profile { return w.byName[username] }

// Post returns the post with pk, or nil.
func (w *World) Post(pk int64) *Post {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.posts[pk]
}

// IDEpochMs is the epoch of Instagram's ids: a media pk holds the
// milliseconds since it in its bits above the 23rd (checked against the
// post pages' taken_at in the owner's probe run, 2026-10-31).
const IDEpochMs = 1314220021721

// MediaPK builds a pk the way Instagram's encode their creation time.
func MediaPK(t time.Time, low int64) int64 {
	return (t.UnixMilli()-IDEpochMs)<<23 | low&(1<<23-1)
}

// shortcode encodes a media pk in Instagram's base64 alphabet.
func shortcode(pk int64) string {
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
	var b []byte
	for v := uint64(pk); v > 0; v /= 64 {
		b = append([]byte{alphabet[v%64]}, b...)
	}
	return string(b)
}
