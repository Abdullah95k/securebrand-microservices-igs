package discovery

import (
	"testing"

	igv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/ig/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
)

type seen struct {
	posts    int
	comments int
}

func (s *seen) Post(*igv1.Post)                      { s.posts++ }
func (s *seen) Comments(_ int64, cs []*igv1.Comment) { s.comments += len(cs) }

// The collector hands its observer (igdisc's recorder) every post and page
// of comments, whatever its own sources: commenters are the recorder's own
// call (it counts them only with pii_mode full).
func TestCollectorHandsPostsAndCommentsOn(t *testing.T) {
	c := NewCollector(nil, config.IGDiscovery{Sources: []string{"tags"}}, false)
	o := &seen{}
	c.Also = o
	c.Post(&igv1.Post{MediaPk: 1, OwnerPk: 2, Usertags: []*igv1.UserTag{{UserPk: 3, Username: "tagged_one"}}})
	c.Post(nil)
	c.Comments(2, []*igv1.Comment{{CommentPk: 4, Author: &igv1.Author{UserPk: 5, Username: "commenter_one"}}})
	c.Comments(2, nil)
	if o.posts != 1 || o.comments != 1 {
		t.Fatalf("observer saw %d posts and %d comments, want 1 and 1", o.posts, o.comments)
	}
	if len(c.pending) != 1 {
		t.Fatalf("collector holds %d accounts, want the tagged one alone", len(c.pending))
	}
	var none *Collector
	none.Post(&igv1.Post{})
	none.Comments(1, []*igv1.Comment{{}})
}
