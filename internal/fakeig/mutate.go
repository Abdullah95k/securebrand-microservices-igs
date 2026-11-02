package fakeig

import (
	"errors"
	"slices"
	"time"
)

// AddPosts publishes n new posts on the profile, each spacing after the
// newest (spacing 0: the world's post spacing), as an account posting
// between two scheduled runs does. It returns them newest first.
func (w *World) AddPosts(username string, n int, spacing time.Duration) ([]*Post, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	p := w.byName[username]
	if p == nil {
		return nil, errors.New("fakeig: no profile " + username)
	}
	if spacing <= 0 {
		spacing = w.spec.PostSpacing
	}
	newest := Epoch
	if len(p.Posts) > 0 {
		newest = p.Posts[0].TakenAt
	}
	i := slices.Index(w.profiles, p)
	var added []*Post
	for k := range n {
		post := w.makePost(p, i, len(p.Posts)+k, newest.Add(time.Duration(k+1)*spacing))
		added = append([]*Post{post}, added...)
		w.posts[post.PK] = post
	}
	p.Posts = append(slices.Clone(added), p.Posts...)
	return added, nil
}

// AddComments adds n top-level comments to a post, newer than its newest.
func (w *World) AddComments(pk int64, n int) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	post := w.posts[pk]
	if post == nil {
		return errors.New("fakeig: no post")
	}
	newest := post.TakenAt
	if len(post.Comments) > 0 {
		newest = post.Comments[0].CreatedAt
	}
	var added []Comment
	for k := range n {
		added = append([]Comment{w.makeComment(newest.Add(time.Duration(k+1)*time.Minute), len(post.Comments)+k+1, k)}, added...)
	}
	post.Comments = append(added, post.Comments...)
	return nil
}

// SetLikes changes a post's like count.
func (w *World) SetLikes(pk, likes int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	post := w.posts[pk]
	if post == nil {
		return errors.New("fakeig: no post")
	}
	post.Likes = likes
	return nil
}

// DeletePost removes a post: it leaves the grid and its page answers 404.
func (w *World) DeletePost(pk int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	post := w.posts[pk]
	if post == nil {
		return errors.New("fakeig: no post")
	}
	post.Deleted = true
	return nil
}

// PostTruth is what a complete read of a post should store.
type PostTruth struct {
	PK       int64
	Code     string
	TakenAt  time.Time
	Pinned   bool
	Gated    bool
	Video    bool
	Carousel int
	// Likes is nil when the counts are hidden.
	Likes        *int64
	CommentCount int64
	// Plays: a video's views (the reels tab), nil for photos.
	Plays    *int64
	Comments []Comment // newest first
}

// ProfileTruth is what a complete read of a profile should store.
type ProfileTruth struct {
	PK         int64
	Username   string
	FullName   string
	Followers  int64
	Verified   bool
	Highlights []Highlight
	Posts      []PostTruth // grid order
}

// Truth returns what a read of the profile back to since should store:
// every visible grid post taken at or after since, and every pinned post.
func (w *World) Truth(username string, since time.Time) (ProfileTruth, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	p := w.byName[username]
	if p == nil {
		return ProfileTruth{}, false
	}
	t := ProfileTruth{PK: p.PK, Username: p.Username, FullName: p.FullName, Followers: p.Followers, Verified: p.Verified,
		Highlights: slices.Clone(p.Highlights)}
	for _, post := range p.Grid() {
		pinned := slices.Contains(p.Pinned, post)
		if !pinned && post.TakenAt.Before(since) {
			continue
		}
		pt := PostTruth{PK: post.PK, Code: post.Code, TakenAt: post.TakenAt, Pinned: pinned, Gated: post.Gated,
			Video: post.MediaType == 2, Carousel: post.CarouselCount, CommentCount: post.CommentCount(),
			Comments: slices.Clone(post.Comments)}
		if !post.LikesHidden {
			l := post.Likes
			pt.Likes = &l
		}
		if post.MediaType == 2 {
			pl := post.Plays
			pt.Plays = &pl
		}
		t.Posts = append(t.Posts, pt)
	}
	return t, true
}
